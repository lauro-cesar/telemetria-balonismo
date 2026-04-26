import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math' as math;

import 'package:flutter/material.dart';
import 'package:geolocator/geolocator.dart';
import 'package:path/path.dart' as p;
import 'package:sqflite/sqflite.dart';
import 'package:uuid/uuid.dart';
import 'package:web_socket_channel/io.dart';
import 'package:web_socket_channel/web_socket_channel.dart';



class GpsTrackerApp extends StatelessWidget {
  const GpsTrackerApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      debugShowCheckedModeBanner: false,
      title: 'GPS Background Tracker',
      theme: ThemeData(
        colorScheme: ColorScheme.fromSeed(seedColor: Colors.green),
        useMaterial3: true,
      ),
      home: const GpsTrackerPage(),
    );
  }
}

class GpsTrackerPage extends StatefulWidget {
  const GpsTrackerPage({super.key});

  @override
  State<GpsTrackerPage> createState() => _GpsTrackerPageState();
}

class _GpsTrackerPageState extends State<GpsTrackerPage> with WidgetsBindingObserver {
  static const String wsUrl = 'wss://balonismo.apiengine.com.br/async-router/balonismo/telemetria/?sso=d31b0295c8c1d6b86311ffa3469f55acc3ac9908';

  static const Duration reconnectDelay = Duration(seconds: 3);
  static const Duration ackTimeout = Duration(seconds: 8);

  final GpsPointStore _store = GpsPointStore();
  final Uuid _uuid = const Uuid();
  final TransformationController _dioramaTransform = TransformationController(
    DioramaViewport.centeredInitialTransform(),
  );

  WebSocketChannel? _channel;
  StreamSubscription<Position>? _positionSubscription;
  Timer? _reconnectTimer;
  Completer<String>? _pendingAckCompleter;
  String? _waitingAckSequence;

  bool _tracking = false;
  bool _appInForeground = true;
  bool _socketConnected = false;
  bool _draining = false;

  int _pendingCount = 0;
  int _sentCount = 0;
  int _sessionPointCount = 0;
  int _reconnectCount = 0;

  Position? _lastPosition;
  String? _currentSessionId;
  String _statusMessage = 'SQLite inicializando...';
  List<GpsPoint> _sessionPoints = [];

  @override
  void initState() {
    super.initState();
    WidgetsBinding.instance.addObserver(this);
    _bootstrap();
  }

  Future<void> _bootstrap() async {
    await _store.open();
    await _refreshCounts();
    _setStatus('SQLite pronto.');
  }

  @override
  void dispose() {
    WidgetsBinding.instance.removeObserver(this);
    _positionSubscription?.cancel();
    _reconnectTimer?.cancel();
    _closeForegroundWebSocket();
    _dioramaTransform.dispose();
    _store.close();
    super.dispose();
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    final bool foreground = state == AppLifecycleState.resumed;
    _appInForeground = foreground;

    if (foreground) {
      _setStatus('App em foreground. WebSocket ativo e fila drenando.');
      _connectWebSocket();
      _drainPendingSerially();
      _refreshSessionPoints();
      return;
    }

    if (state == AppLifecycleState.inactive ||
        state == AppLifecycleState.paused ||
        state == AppLifecycleState.hidden ||
        state == AppLifecycleState.detached) {
      _setStatus('App em background. WebSocket fechado; GPS continua coletando.');
      _closeForegroundWebSocket();
    }
  }

  Future<void> _startTracking() async {
    if (_tracking) return;

    final bool allowed = await _ensureLocationPermission();
    if (!allowed) return;

    final String sessionId = _uuid.v4();

    setState(() {
      _tracking = true;
      _currentSessionId = sessionId;
      _sessionPoints = [];
      _sessionPointCount = 0;
      _lastPosition = null;
      _dioramaTransform.value = DioramaViewport.centeredInitialTransform();
    });

    _startContinuousLocationStream();

    if (_appInForeground) {
      _connectWebSocket();
    }

    _setStatus('Sessão iniciada: $sessionId');
  }

  Future<void> _stopTracking() async {
    _tracking = false;
    _reconnectTimer?.cancel();
    _reconnectTimer = null;

    await _positionSubscription?.cancel();
    _positionSubscription = null;

    await _closeForegroundWebSocket();

    if (mounted) {
      setState(() => _socketConnected = false);
    }

    _setStatus('Tracking parado. Pontos pendentes permanecem no SQLite.');
  }

  Future<bool> _ensureLocationPermission() async {
    final bool serviceEnabled = await Geolocator.isLocationServiceEnabled();
    if (!serviceEnabled) {
      _setStatus('Serviço de localização desativado no aparelho.');
      return false;
    }

    LocationPermission permission = await Geolocator.checkPermission();

    if (permission == LocationPermission.denied) {
      permission = await Geolocator.requestPermission();
    }

    if (permission == LocationPermission.denied) {
      _setStatus('Permissão de localização negada.');
      return false;
    }

    if (permission == LocationPermission.deniedForever) {
      _setStatus('Permissão negada permanentemente. Abrindo configurações do app.');
      await Geolocator.openAppSettings();
      return false;
    }

    if (permission != LocationPermission.always) {
      _setStatus('Para background real, conceda permissão de localização "Sempre".');
    }

    return true;
  }

  void _startContinuousLocationStream() {
     AndroidSettings androidSettings = AndroidSettings(
      accuracy: LocationAccuracy.bestForNavigation,
      distanceFilter: 3,
      intervalDuration: Duration(seconds: 2),
      foregroundNotificationConfig: ForegroundNotificationConfig(
        notificationTitle: 'GPS Tracker ativo',
        notificationText: 'Coletando localização em segundo plano.',
        enableWakeLock: true,
        setOngoing: true,
      ),
    );

    AppleSettings appleSettings = AppleSettings(
      accuracy: LocationAccuracy.bestForNavigation,
      activityType: ActivityType.automotiveNavigation,
      distanceFilter: 3,
      pauseLocationUpdatesAutomatically: false,
      showBackgroundLocationIndicator: true,
      allowBackgroundLocationUpdates: true,
    );

    final LocationSettings locationSettings = Platform.isAndroid ? androidSettings : appleSettings;

    _positionSubscription?.cancel();
    _positionSubscription = Geolocator.getPositionStream(
      locationSettings: locationSettings,
    ).listen(
          (Position position) async {
        final String? sessionId = _currentSessionId;
        if (sessionId == null) return;

        final GpsPoint point = _buildPoint(position, sessionId: sessionId);
        await _store.insertPending(point);

        if (!mounted) return;
        setState(() {
          _lastPosition = position;
        });

        await _refreshSessionPoints();
        await _refreshCounts();

        _setStatus('Ponto da sessão persistido: ${point.sequence}');

        if (_appInForeground && _socketConnected) {
          _drainPendingSerially();
        }
      },
      onError: (Object error) {
        _setStatus('Erro no stream de localização: $error');
      },
      cancelOnError: false,
    );
  }

  GpsPoint _buildPoint(Position position, {required String sessionId}) {
    final DateTime capturedAt = position.timestamp?.toUtc() ?? DateTime.now().toUtc();
    final String sequence = _uuid.v4();

    return GpsPoint(
      sequence: sequence,
      sessionId: sessionId,
      capturedAt: capturedAt,
      latitude: position.latitude,
      longitude: position.longitude,
      altitude: position.altitude,
      accuracy: position.accuracy,
      heading: position.heading,
      speed: position.speed,
      payload: {
        'type': 'gps_position',
        'sequence': sequence,
        'session_id': sessionId,
        'captured_at': capturedAt.toIso8601String(),
        'coords': {
          'lat': position.latitude,
          'lon': position.longitude,
          'altitude': position.altitude,
          'accuracy': position.accuracy,
          'altitude_accuracy': position.altitudeAccuracy,
          'heading': position.heading,
          'speed': position.speed,
        },
      },
    );
  }

  void _connectWebSocket() {
    if (!_tracking || !_appInForeground || _socketConnected) return;

    try {
      _setStatus('Conectando WebSocket em foreground...');
      _channel = IOWebSocketChannel.connect(Uri.parse(wsUrl));

      _channel!.stream.listen(
        _handleServerMessage,
        onDone: () => _handleSocketDisconnected('WebSocket fechado.'),
        onError: (Object error) => _handleSocketDisconnected('Erro no WebSocket: $error'),
        cancelOnError: true,
      );

      setState(() => _socketConnected = true);
      _setStatus('WebSocket conectado.');
      _drainPendingSerially();
    } catch (error) {
      _handleSocketDisconnected('Falha ao conectar WebSocket: $error');
    }
  }

  void _handleServerMessage(dynamic message) {
    final String? ackSequence = _extractAckSequence(message);
    if (ackSequence == null) return;

    final Completer<String>? completer = _pendingAckCompleter;
    if (_waitingAckSequence == ackSequence && completer != null && !completer.isCompleted) {
      completer.complete(ackSequence);
    }
  }

  String? _extractAckSequence(dynamic message) {
    if (message is String) {
      final String trimmed = message.trim();
      if (trimmed.isEmpty) return null;

      try {
        final dynamic decoded = jsonDecode(trimmed);
        if (decoded is Map<String, dynamic>) {
          final dynamic value = decoded['ack'] ?? decoded['sequence'] ?? decoded['sequence_id'];
          return value?.toString();
        }
      } catch (_) {
        return trimmed;
      }
    }

    if (message is List<int>) {
      return _extractAckSequence(utf8.decode(message));
    }

    return null;
  }

  void _handleSocketDisconnected(String reason) {
    _setStatus(reason);

    final Completer<String>? completer = _pendingAckCompleter;
    if (completer != null && !completer.isCompleted) {
      completer.completeError(StateError('Socket desconectado antes do ACK.'));
    }

    _pendingAckCompleter = null;
    _waitingAckSequence = null;

    try {
      _channel?.sink.close();
    } catch (_) {}

    _channel = null;

    if (mounted) {
      setState(() => _socketConnected = false);
    }

    if (_tracking && _appInForeground) {
      _scheduleReconnect();
    }
  }

  void _scheduleReconnect() {
    if (_reconnectTimer != null) return;

    _reconnectTimer = Timer(reconnectDelay, () {
      _reconnectTimer = null;
      _reconnectCount += 1;
      _setStatus('Reconectando WebSocket. Tentativa $_reconnectCount.');
      _connectWebSocket();
    });
  }

  Future<void> _closeForegroundWebSocket() async {
    _reconnectTimer?.cancel();
    _reconnectTimer = null;

    final Completer<String>? completer = _pendingAckCompleter;
    if (completer != null && !completer.isCompleted) {
      completer.completeError(StateError('WebSocket fechado antes do ACK.'));
    }

    _pendingAckCompleter = null;
    _waitingAckSequence = null;

    try {
      await _channel?.sink.close();
    } catch (_) {}

    _channel = null;

    if (mounted) {
      setState(() => _socketConnected = false);
    }
  }

  Future<void> _drainPendingSerially() async {
    if (_draining || !_appInForeground || !_socketConnected || _channel == null) return;

    _draining = true;

    try {
      while (_appInForeground && _socketConnected && _channel != null) {
        final GpsPoint? next = await _store.nextPending();
        if (next == null) break;

        try {
          await _sendAndWaitAck(next);
          await _store.markSent(next.id!);
          await _refreshCounts();
          _setStatus('ACK recebido: ${next.sequence}');
        } catch (_) {
          _setStatus('Envio pausado. Ponto continua pending: ${next.sequence}');
          break;
        }
      }
    } finally {
      _draining = false;
    }
  }

  Future<void> _sendAndWaitAck(GpsPoint point) async {
    final WebSocketChannel? channel = _channel;
    if (channel == null || !_socketConnected) {
      throw StateError('WebSocket não conectado.');
    }

    _waitingAckSequence = point.sequence;
    _pendingAckCompleter = Completer<String>();

    channel.sink.add(jsonEncode(point.payload));

    final String ack = await _pendingAckCompleter!.future.timeout(
      ackTimeout,
      onTimeout: () => throw TimeoutException('ACK não recebido para ${point.sequence}.', ackTimeout),
    );

    _pendingAckCompleter = null;
    _waitingAckSequence = null;

    if (ack != point.sequence) {
      throw StateError('ACK inesperado: $ack, esperado: ${point.sequence}');
    }
  }

  Future<void> _refreshCounts() async {
    final int pending = await _store.countByStatus('pending');
    final int sent = await _store.countByStatus('sent');
    final String? sessionId = _currentSessionId;
    final int sessionCount = sessionId == null ? 0 : await _store.countBySession(sessionId);

    if (!mounted) return;
    setState(() {
      _pendingCount = pending;
      _sentCount = sent;
      _sessionPointCount = sessionCount;
    });
  }

  Future<void> _refreshSessionPoints() async {
    final String? sessionId = _currentSessionId;
    if (sessionId == null) return;

    final List<GpsPoint> points = await _store.pointsBySession(sessionId);
    if (!mounted) return;

    setState(() {
      _sessionPoints = points;
      _sessionPointCount = points.length;
    });

    _centerDioramaOnLastPoint();
  }

  void _zoomDiorama(double factor) {
    final double currentScale = _dioramaTransform.value.getMaxScaleOnAxis();
    final double nextScale = (currentScale * factor).clamp(0.35, 8.0).toDouble();
    _centerDioramaOnLastPoint(scale: nextScale);
  }

  void _resetDioramaZoom() {
    _centerDioramaOnLastPoint(scale: 1.0);
  }

  void _centerDioramaOnLastPoint({double? scale}) {
    final double nextScale = (scale ?? _dioramaTransform.value.getMaxScaleOnAxis()).clamp(0.35, 8.0).toDouble();

    if (_sessionPoints.isEmpty) {
      _dioramaTransform.value = DioramaViewport.centeredInitialTransform(scale: nextScale);
      return;
    }

    final Offset target = DioramaViewport.projectPoint(
      points: _sessionPoints,
      point: _sessionPoints.last,
    );

    _dioramaTransform.value = DioramaViewport.centerTransformOnCanvasPoint(
      target,
      scale: nextScale,
    );
  }

  void _setStatus(String message) {
    if (!mounted) return;
    setState(() => _statusMessage = message);
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('GPS Background Tracker')),
      body: ListView(
        padding: const EdgeInsets.all(16),
        children: [
          Card(
            child: Padding(
              padding: const EdgeInsets.all(16),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    _tracking ? 'Tracking contínuo ativo' : 'Tracking parado',
                    style: Theme.of(context).textTheme.headlineSmall,
                  ),
                  const SizedBox(height: 12),
                  Wrap(
                    spacing: 8,
                    runSpacing: 8,
                    children: [
                      FilledButton.icon(
                        onPressed: _tracking ? null : _startTracking,
                        icon: const Icon(Icons.play_arrow),
                        label: const Text('Start'),
                      ),
                      FilledButton.tonalIcon(
                        onPressed: _tracking ? _stopTracking : null,
                        icon: const Icon(Icons.stop),
                        label: const Text('Stop'),
                      ),
                    ],
                  ),
                  const SizedBox(height: 16),
                  _InfoRow(label: 'Sessão', value: _currentSessionId ?? '-'),
                  _InfoRow(label: 'Pontos sessão', value: '$_sessionPointCount'),
                  _InfoRow(label: 'Lifecycle', value: _appInForeground ? 'foreground' : 'background'),
                  _InfoRow(label: 'WebSocket', value: _socketConnected ? 'conectado' : 'fechado'),
                  _InfoRow(label: 'SQLite pending', value: '$_pendingCount'),
                  _InfoRow(label: 'SQLite sent', value: '$_sentCount'),
                  _InfoRow(label: 'Reconexões', value: '$_reconnectCount'),
                  _InfoRow(label: 'Status', value: _statusMessage),
                ],
              ),
            ),
          ),
          const SizedBox(height: 12),
          PositionDioramaCard(
            points: _sessionPoints,
            controller: _dioramaTransform,
            onZoomIn: () => _zoomDiorama(1.25),
            onZoomOut: () => _zoomDiorama(0.8),
            onResetZoom: _resetDioramaZoom,
          ),
        ],
      ),
    );
  }
}

class PositionDioramaCard extends StatelessWidget {
  const PositionDioramaCard({
    super.key,
    required this.points,
    required this.controller,
    required this.onZoomIn,
    required this.onZoomOut,
    required this.onResetZoom,
  });

  final List<GpsPoint> points;
  final TransformationController controller;
  final VoidCallback onZoomIn;
  final VoidCallback onZoomOut;
  final VoidCallback onResetZoom;

  @override
  Widget build(BuildContext context) {
    final GpsPoint? last = points.isNotEmpty ? points.last : null;

    return Card(
      clipBehavior: Clip.antiAlias,
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                Expanded(
                  child: Text('Diorama da sessão', style: Theme.of(context).textTheme.titleLarge),
                ),
                IconButton.filledTonal(
                  tooltip: 'Zoom out',
                  onPressed: onZoomOut,
                  icon: const Icon(Icons.remove),
                ),
                const SizedBox(width: 6),
                IconButton.filledTonal(
                  tooltip: 'Resetar zoom',
                  onPressed: onResetZoom,
                  icon: const Icon(Icons.center_focus_strong),
                ),
                const SizedBox(width: 6),
                IconButton.filledTonal(
                  tooltip: 'Zoom in',
                  onPressed: onZoomIn,
                  icon: const Icon(Icons.add),
                ),
              ],
            ),
            const SizedBox(height: 8),
            Text(
              last == null
                  ? 'Aguardando primeira coordenada da sessão...'
                  : '${points.length} pontos · Lat ${last.latitude.toStringAsFixed(6)}, Lon ${last.longitude.toStringAsFixed(6)}, Alt ${last.altitude.toStringAsFixed(1)} m',
              style: Theme.of(context).textTheme.bodyMedium,
            ),
            const SizedBox(height: 12),
            Center(
              child: SizedBox(
                height: DioramaViewport.visibleHeight,
                width: DioramaViewport.visibleWidth,
                child: ClipRRect(
                  borderRadius: BorderRadius.circular(20),
                  child: DecoratedBox(
                    decoration: BoxDecoration(
                      gradient: LinearGradient(
                        begin: Alignment.topCenter,
                        end: Alignment.bottomCenter,
                        colors: [
                          Theme.of(context).colorScheme.primaryContainer.withOpacity(0.45),
                          Theme.of(context).colorScheme.surfaceContainerHighest,
                        ],
                      ),
                    ),
                    child: InteractiveViewer(
                      transformationController: controller,
                      minScale: 0.35,
                      maxScale: 8,
                      boundaryMargin: const EdgeInsets.all(900),
                      constrained: false,
                      child: SizedBox(
                        width: DioramaViewport.canvasWidth,
                        height: DioramaViewport.canvasHeight,
                        child: CustomPaint(
                          painter: DioramaSessionPainter(points: points),
                        ),
                      ),
                    ),
                  ),
                ),
              ),
            ),
            const SizedBox(height: 8),
            Text(
              'Área estimada: 10km × 10km. O último ponto permanece centralizado; use zoom para aproximar/afastar.',
              style: Theme.of(context).textTheme.bodySmall,
            ),
          ],
        ),
      ),
    );
  }
}

class DioramaViewport {
  static const double canvasWidth = 1800;
  static const double canvasHeight = 1200;
  static const double visibleWidth = 360;
  static const double visibleHeight = 360;
  static const double areaMeters = 10000;
  static const double metersPerDegreeLat = 111320;

  static Offset get canvasCenter => const Offset(canvasWidth / 2, canvasHeight / 2 + 80);

  static Offset projectPoint({required List<GpsPoint> points, required GpsPoint point}) {
    if (points.isEmpty) return canvasCenter;

    final GpsPoint origin = points.first;
    final double cosLat = math.cos(origin.latitude * math.pi / 180).abs().clamp(0.1, 1.0).toDouble();
    final double metersX = (point.longitude - origin.longitude) * metersPerDegreeLat * cosLat;
    final double metersY = (point.latitude - origin.latitude) * metersPerDegreeLat;
    final double pixelsPerMeter = (canvasWidth * 0.72) / areaMeters;

    return Offset(
      canvasCenter.dx + metersX * pixelsPerMeter,
      canvasCenter.dy - metersY * pixelsPerMeter,
    );
  }

  static Matrix4 centeredInitialTransform({double scale = 1.0}) {
    return centerTransformOnCanvasPoint(canvasCenter, scale: scale);
  }

  static Matrix4 centerTransformOnCanvasPoint(Offset canvasPoint, {required double scale}) {
    final double safeScale = scale.clamp(0.35, 8.0).toDouble();
    final double tx = visibleWidth / 2 - canvasPoint.dx * safeScale;
    final double ty = visibleHeight / 2 - canvasPoint.dy * safeScale;

    return Matrix4.identity()
      ..translate(tx, ty)
      ..scale(safeScale);
  }
}

class DioramaSessionPainter extends CustomPainter {
  DioramaSessionPainter({required this.points});

  final List<GpsPoint> points;

  @override
  void paint(Canvas canvas, Size size) {
    final Offset center = DioramaViewport.canvasCenter;
    final double tileW = size.width * 0.74;
    final double tileH = size.height * 0.46;

    _drawSceneBase(canvas, center, tileW, tileH);
    _drawGrid(canvas, center);

    if (points.isEmpty) {
      _drawEmptyState(canvas, size);
      return;
    }

    final List<Offset> projected = points
        .map((GpsPoint point) => DioramaViewport.projectPoint(points: points, point: point))
        .toList();

    final List<Offset> visiblePoints = _spreadOverlappingPoints(projected);

    _drawPath(canvas, visiblePoints);
    _drawPoints(canvas, visiblePoints, points);
    _drawLastMarker(canvas, visiblePoints.last, points.last);
    _drawScaleLegend(canvas, size, points);
  }

  void _drawSceneBase(Canvas canvas, Offset center, double tileW, double tileH) {
    final Paint paint = Paint()..isAntiAlias = true;

    final Path ground = Path()
      ..moveTo(center.dx, center.dy - tileH / 2)
      ..lineTo(center.dx + tileW / 2, center.dy)
      ..lineTo(center.dx, center.dy + tileH / 2)
      ..lineTo(center.dx - tileW / 2, center.dy)
      ..close();

    final Path rightWall = Path()
      ..moveTo(center.dx, center.dy + tileH / 2)
      ..lineTo(center.dx + tileW / 2, center.dy)
      ..lineTo(center.dx + tileW / 2, center.dy + 170)
      ..lineTo(center.dx, center.dy + tileH / 2 + 170)
      ..close();

    final Path leftWall = Path()
      ..moveTo(center.dx, center.dy + tileH / 2)
      ..lineTo(center.dx - tileW / 2, center.dy)
      ..lineTo(center.dx - tileW / 2, center.dy + 170)
      ..lineTo(center.dx, center.dy + tileH / 2 + 170)
      ..close();

    paint.color = const Color(0x33222222);
    canvas.drawOval(
      Rect.fromCenter(
        center: Offset(center.dx, center.dy + tileH * 0.58),
        width: tileW * 0.92,
        height: tileH * 0.36,
      ),
      paint,
    );

    paint.color = const Color(0xFF78B94B);
    canvas.drawPath(ground, paint);
    paint.color = const Color(0xFF4B8F32);
    canvas.drawPath(rightWall, paint);
    paint.color = const Color(0xFF3F7A2D);
    canvas.drawPath(leftWall, paint);
  }

  void _drawGrid(Canvas canvas, Offset center) {
    final Paint paint = Paint()
      ..isAntiAlias = true
      ..style = PaintingStyle.stroke
      ..strokeWidth = 1
      ..color = const Color(0x337CFC00);

    final double halfMeters = DioramaViewport.areaMeters / 2;
    final double pixelsPerMeter = (DioramaViewport.canvasWidth * 0.72) / DioramaViewport.areaMeters;

    for (double meter = -halfMeters; meter <= halfMeters; meter += 1000) {
      final double offset = meter * pixelsPerMeter;
      canvas.drawLine(
        Offset(center.dx + offset, center.dy - halfMeters * pixelsPerMeter),
        Offset(center.dx + offset, center.dy + halfMeters * pixelsPerMeter),
        paint,
      );
      canvas.drawLine(
        Offset(center.dx - halfMeters * pixelsPerMeter, center.dy + offset),
        Offset(center.dx + halfMeters * pixelsPerMeter, center.dy + offset),
        paint,
      );
    }
  }

  List<Offset> _spreadOverlappingPoints(List<Offset> projected) {
    if (projected.length <= 1) return projected;

    final List<Offset> result = [];
    final Map<String, int> bucketCount = {};

    for (final Offset point in projected) {
      final String bucket = '${(point.dx / 6).round()}:${(point.dy / 6).round()}';
      final int index = bucketCount[bucket] ?? 0;
      bucketCount[bucket] = index + 1;

      if (index == 0) {
        result.add(point);
        continue;
      }

      final double angle = index * 0.85;
      final double radius = math.min(18, 4 + index * 1.8);
      result.add(point.translate(math.cos(angle) * radius, math.sin(angle) * radius));
    }

    return result;
  }
  void _drawPath(Canvas canvas, List<Offset> projected) {
    if (projected.length < 2) return;

    final Path path = Path()..moveTo(projected.first.dx, projected.first.dy);
    for (final Offset point in projected.skip(1)) {
      path.lineTo(point.dx, point.dy);
    }

    final Paint shadowPaint = Paint()
      ..isAntiAlias = true
      ..style = PaintingStyle.stroke
      ..strokeWidth = 9
      ..strokeCap = StrokeCap.round
      ..strokeJoin = StrokeJoin.round
      ..color = const Color(0x55222222);

    final Paint linePaint = Paint()
      ..isAntiAlias = true
      ..style = PaintingStyle.stroke
      ..strokeWidth = 3
      ..strokeCap = StrokeCap.round
      ..strokeJoin = StrokeJoin.round
      ..color = const Color(0xFF2563EB);

    canvas.drawPath(path.shift(const Offset(0, 5)), shadowPaint);
    canvas.drawPath(path, linePaint);
  }

  void _drawPoints(Canvas canvas, List<Offset> projected, List<GpsPoint> points) {
    final Paint paint = Paint()..isAntiAlias = true;

    for (int i = 0; i < projected.length; i++) {
      final Offset point = projected[i];
      final double altitude = points[i].altitude;
      final double z = altitude.clamp(0, 250).toDouble() / 250 * 90;
      final Offset top = point.translate(0, -z);
      final bool isFirst = i == 0;
      final bool isLast = i == projected.length - 1;

      paint
        ..style = PaintingStyle.stroke
        ..strokeWidth = 1.4
        ..color = const Color(0x88334155);
      canvas.drawLine(point, top, paint);

      paint
        ..style = PaintingStyle.fill
        ..color = isFirst
            ? const Color(0xFF22C55E)
            : isLast
            ? const Color(0xFFF43F5E)
            : const Color(0xFF0EA5E9);
      canvas.drawCircle(top, isFirst || isLast ? 7 : 5, paint);

      paint
        ..style = PaintingStyle.stroke
        ..strokeWidth = 2
        ..color = const Color(0xFFFFFFFF);
      canvas.drawCircle(top, isFirst || isLast ? 7 : 5, paint);
    }
  }

  void _drawLastMarker(Canvas canvas, Offset base, GpsPoint point) {
    final double altitudeHeight = point.altitude.clamp(0, 250).toDouble() / 250 * 70 + 24;
    final Offset top = base.translate(0, -altitudeHeight);
    final Paint paint = Paint()..isAntiAlias = true;

    paint
      ..style = PaintingStyle.stroke
      ..strokeWidth = 5
      ..strokeCap = StrokeCap.round
      ..color = const Color(0xFF1E293B);
    canvas.drawLine(base, top, paint);

    paint
      ..style = PaintingStyle.fill
      ..color = const Color(0xFFE11D48);
    canvas.drawCircle(top, 8, paint);

    paint.color = const Color(0xFFFFE4E6);
    canvas.drawCircle(top.translate(-2.5, -2.5), 2.5, paint);

    _drawHeadingArrow(canvas, top, point.heading, 28);
    _drawLabel(canvas, top.translate(12, -20), 'Atual ${point.altitude.toStringAsFixed(0)} m');
  }

  void _drawHeadingArrow(Canvas canvas, Offset origin, double heading, double length) {
    if (heading.isNaN || heading < 0) return;

    final double angle = (heading - 90) * math.pi / 180;
    final Offset end = origin + Offset(math.cos(angle), math.sin(angle)) * length;

    final Paint paint = Paint()
      ..isAntiAlias = true
      ..style = PaintingStyle.stroke
      ..strokeWidth = 2.4
      ..strokeCap = StrokeCap.round
      ..color = const Color(0xFF1D4ED8);

    canvas.drawLine(origin, end, paint);

    final Offset left = end + Offset(math.cos(angle + 2.5), math.sin(angle + 2.5)) * 8;
    final Offset right = end + Offset(math.cos(angle - 2.5), math.sin(angle - 2.5)) * 14;
    canvas.drawLine(end, left, paint);
    canvas.drawLine(end, right, paint);
  }

  void _drawLabel(Canvas canvas, Offset offset, String label) {
    final TextPainter textPainter = TextPainter(
      text: TextSpan(
        text: label,
        style: const TextStyle(
          color: Color(0xFF0F172A),
          fontSize: 12,
          fontWeight: FontWeight.bold,
        ),
      ),
      textDirection: TextDirection.ltr,
    )..layout();

    final RRect bg = RRect.fromRectAndRadius(
      Rect.fromLTWH(offset.dx - 6, offset.dy - 4, textPainter.width + 12, textPainter.height + 8),
      const Radius.circular(8),
    );

    final Paint paint = Paint()..color = const Color(0xDDFFFFFF);
    canvas.drawRRect(bg, paint);
    textPainter.paint(canvas, offset);
  }

  void _drawScaleLegend(Canvas canvas, Size size, List<GpsPoint> points) {
    final TextPainter textPainter = TextPainter(
      text: TextSpan(
        text: 'Sessão: ${points.length} pontos · área 10km×10km · último ponto centralizado',
        style: const TextStyle(
          color: Color(0xFF334155),
          fontSize: 24,
          fontWeight: FontWeight.w600,
        ),
      ),
      textDirection: TextDirection.ltr,
    )..layout(maxWidth: size.width - 80);

    textPainter.paint(canvas, const Offset(40, 38));
  }

  void _drawEmptyState(Canvas canvas, Size size) {
    final TextPainter textPainter = TextPainter(
      text: const TextSpan(
        text: 'Sem pontos nesta sessão',
        style: TextStyle(
          color: Color(0xFF334155),
          fontSize: 32,
          fontWeight: FontWeight.bold,
        ),
      ),
      textDirection: TextDirection.ltr,
    )..layout();

    textPainter.paint(
      canvas,
      Offset((size.width - textPainter.width) / 2, size.height * 0.25),
    );
  }

  @override
  bool shouldRepaint(covariant DioramaSessionPainter oldDelegate) {
    return oldDelegate.points != points;
  }
}

class GpsPoint {
  GpsPoint({
    this.id,
    required this.sequence,
    required this.sessionId,
    required this.capturedAt,
    required this.latitude,
    required this.longitude,
    required this.altitude,
    required this.accuracy,
    required this.heading,
    required this.speed,
    required this.payload,
    this.status = 'pending',
  });

  final int? id;
  final String sequence;
  final String sessionId;
  final DateTime capturedAt;
  final double latitude;
  final double longitude;
  final double altitude;
  final double accuracy;
  final double heading;
  final double speed;
  final Map<String, dynamic> payload;
  final String status;

  Map<String, dynamic> toDb() {
    return {
      'sequence': sequence,
      'session_id': sessionId,
      'captured_at': capturedAt.toUtc().toIso8601String(),
      'latitude': latitude,
      'longitude': longitude,
      'altitude': altitude,
      'accuracy': accuracy,
      'heading': heading,
      'speed': speed,
      'payload_json': jsonEncode(payload),
      'status': status,
      'created_at': DateTime.now().toUtc().toIso8601String(),
      'sent_at': null,
    };
  }

  factory GpsPoint.fromDb(Map<String, Object?> row) {
    return GpsPoint(
      id: row['id'] as int,
      sequence: row['sequence'] as String,
      sessionId: row['session_id'] as String,
      capturedAt: DateTime.parse(row['captured_at'] as String),
      latitude: (row['latitude'] as num).toDouble(),
      longitude: (row['longitude'] as num).toDouble(),
      altitude: (row['altitude'] as num?)?.toDouble() ?? 0,
      accuracy: (row['accuracy'] as num?)?.toDouble() ?? 0,
      heading: (row['heading'] as num?)?.toDouble() ?? -1,
      speed: (row['speed'] as num?)?.toDouble() ?? 0,
      payload: jsonDecode(row['payload_json'] as String) as Map<String, dynamic>,
      status: row['status'] as String,
    );
  }
}

class GpsPointStore {
  Database? _db;

  Future<void> open() async {
    if (_db != null) return;

    final String dbPath = await getDatabasesPath();
    final String path = p.join(dbPath, 'gps_points.db');

    _db = await openDatabase(
      path,
      version: 2,
      onCreate: _createSchema,
      onUpgrade: (Database db, int oldVersion, int newVersion) async {
        if (oldVersion < 2) {
          await db.execute('DROP TABLE IF EXISTS gps_points');
          await _createSchema(db, newVersion);
        }
      },
    );
  }

  Future<void> _createSchema(Database db, int version) async {
    await db.execute('''
      CREATE TABLE gps_points (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sequence TEXT NOT NULL UNIQUE,
        session_id TEXT NOT NULL,
        captured_at TEXT NOT NULL,
        latitude REAL NOT NULL,
        longitude REAL NOT NULL,
        altitude REAL NOT NULL DEFAULT 0,
        accuracy REAL NOT NULL DEFAULT 0,
        heading REAL NOT NULL DEFAULT -1,
        speed REAL NOT NULL DEFAULT 0,
        payload_json TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at TEXT NOT NULL,
        sent_at TEXT
      )
    ''');

    await db.execute('''
      CREATE INDEX idx_gps_points_status_order
      ON gps_points(status, id)
    ''');

    await db.execute('''
      CREATE INDEX idx_gps_points_session_order
      ON gps_points(session_id, id)
    ''');
  }

  Future<void> close() async {
    await _db?.close();
    _db = null;
  }

  Future<void> insertPending(GpsPoint point) async {
    final Database db = _requireDb();
    await db.insert(
      'gps_points',
      point.toDb(),
      conflictAlgorithm: ConflictAlgorithm.ignore,
    );
  }

  Future<GpsPoint?> nextPending() async {
    final Database db = _requireDb();
    final List<Map<String, Object?>> rows = await db.query(
      'gps_points',
      where: 'status = ?',
      whereArgs: ['pending'],
      orderBy: 'id ASC',
      limit: 1,
    );

    if (rows.isEmpty) return null;
    return GpsPoint.fromDb(rows.first);
  }

  Future<List<GpsPoint>> pointsBySession(String sessionId) async {
    final Database db = _requireDb();
    final List<Map<String, Object?>> rows = await db.query(
      'gps_points',
      where: 'session_id = ?',
      whereArgs: [sessionId],
      orderBy: 'id ASC',
    );
    return rows.map(GpsPoint.fromDb).toList();
  }

  Future<void> markSent(int id) async {
    final Database db = _requireDb();
    await db.update(
      'gps_points',
      {
        'status': 'sent',
        'sent_at': DateTime.now().toUtc().toIso8601String(),
      },
      where: 'id = ?',
      whereArgs: [id],
    );
  }

  Future<int> countByStatus(String status) async {
    final Database db = _requireDb();
    final List<Map<String, Object?>> result = await db.rawQuery(
      'SELECT COUNT(*) AS total FROM gps_points WHERE status = ?',
      [status],
    );
    return Sqflite.firstIntValue(result) ?? 0;
  }

  Future<int> countBySession(String sessionId) async {
    final Database db = _requireDb();
    final List<Map<String, Object?>> result = await db.rawQuery(
      'SELECT COUNT(*) AS total FROM gps_points WHERE session_id = ?',
      [sessionId],
    );
    return Sqflite.firstIntValue(result) ?? 0;
  }

  Database _requireDb() {
    final Database? db = _db;
    if (db == null) {
      throw StateError('Banco SQLite ainda não foi inicializado.');
    }
    return db;
  }
}

class _InfoRow extends StatelessWidget {
  const _InfoRow({required this.label, required this.value});

  final String label;
  final String value;

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 4),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          SizedBox(
            width: 130,
            child: Text(
              label,
              style: const TextStyle(fontWeight: FontWeight.bold),
            ),
          ),
          Expanded(child: Text(value)),
        ],
      ),
    );
  }
}
