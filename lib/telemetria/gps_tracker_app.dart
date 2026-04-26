import 'dart:async';
import 'dart:convert';
import 'dart:io';
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

  final GpsPointStore _store = GpsPointStore();
  final _uuid = const Uuid();
  final List<String> _logs = [];

  WebSocketChannel? _channel;
  StreamSubscription<Position>? _positionSubscription;
  Timer? _reconnectTimer;

  bool _tracking = false;
  bool _appInForeground = true;
  bool _socketConnected = false;
  bool _draining = false;
  int _pendingCount = 0;
  int _sentCount = 0;
  int _reconnectCount = 0;
  Position? _lastPosition;

  @override
  void initState() {
    super.initState();
    WidgetsBinding.instance.addObserver(this);
    _bootstrap();
  }

  Future<void> _bootstrap() async {
    await _store.open();
    await _refreshCounts();
    _log('SQLite inicializado.');
  }

  @override
  void dispose() {
    WidgetsBinding.instance.removeObserver(this);
    _stopTracking();
    _store.close();
    super.dispose();
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    final foreground = state == AppLifecycleState.resumed;

    _appInForeground = foreground;

    if (foreground) {
      _log('App voltou para foreground. Abrindo WebSocket e drenando fila.');
      _connectWebSocket();
      _drainPendingSerially();
      return;
    }

    if (state == AppLifecycleState.inactive ||
        state == AppLifecycleState.paused ||
        state == AppLifecycleState.hidden ||
        state == AppLifecycleState.detached) {
      _log('App saiu do foreground. Fechando WebSocket; GPS continua coletando.');
      _closeForegroundWebSocket();
    }
  }

  Future<void> _startTracking() async {
    if (_tracking) return;

    final allowed = await _ensureLocationPermission();
    if (!allowed) return;

    setState(() => _tracking = true);

    _startContinuousLocationStream();

    if (_appInForeground) {
      _connectWebSocket();
    }

    _log('Tracking contínuo iniciado.');
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

    _log('Tracking parado. Pontos pendentes permanecem no SQLite.');
  }

  Future<bool> _ensureLocationPermission() async {
    final serviceEnabled = await Geolocator.isLocationServiceEnabled();
    if (!serviceEnabled) {
      _log('Serviço de localização desativado no aparelho.');
      return false;
    }

    var permission = await Geolocator.checkPermission();

    if (permission == LocationPermission.denied) {
      permission = await Geolocator.requestPermission();
    }

    if (permission == LocationPermission.denied) {
      _log('Permissão de localização negada.');
      return false;
    }

    if (permission == LocationPermission.deniedForever) {
      _log('Permissão negada permanentemente. Abrindo configurações do app.');
      await Geolocator.openAppSettings();
      return false;
    }

    if (permission != LocationPermission.always) {
      _log('Atenção: para background real, conceda permissão de localização "Sempre".');
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

    final locationSettings = Platform.isAndroid ? androidSettings : appleSettings;

    _positionSubscription = Geolocator.getPositionStream(
      locationSettings: locationSettings,
    ).listen(
          (position) async {
        final point = _buildPoint(position);
        await _store.insertPending(point);

        if (!mounted) return;
        setState(() => _lastPosition = position);
        await _refreshCounts();

        _log('GPS coletado e persistido no SQLite: ${point.sequence}');

        if (_appInForeground && _socketConnected) {
          _drainPendingSerially();
        }
      },
      onError: (error) {
        _log('Erro no stream de localização: $error');
      },
      cancelOnError: false,
    );
  }

  GpsPoint _buildPoint(Position position) {
    final capturedAt = position.timestamp?.toUtc() ?? DateTime.now().toUtc();

    return GpsPoint(
      sequence: _uuid.v4(),
      capturedAt: capturedAt,
      payload: {
        'type': 'gps_position',
        'sequence': _uuid.v4(),
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
      _log('Conectando WebSocket em foreground: $wsUrl');
      _channel = IOWebSocketChannel.connect(Uri.parse(wsUrl));

      _channel!.stream.listen(
            (message) {
          _log('Servidor respondeu: $message');
        },
        onDone: () {
          _handleSocketDisconnected('WebSocket fechado.');
        },
        onError: (error) {
          _handleSocketDisconnected('Erro no WebSocket: $error');
        },
        cancelOnError: true,
      );

      setState(() => _socketConnected = true);
      _log('WebSocket conectado.');
      _drainPendingSerially();
    } catch (error) {
      _handleSocketDisconnected('Falha ao conectar WebSocket: $error');
    }
  }

  void _handleSocketDisconnected(String reason) {
    _log(reason);

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
      _log('Tentando reconectar WebSocket. Tentativa $_reconnectCount.');
      _connectWebSocket();
    });
  }

  Future<void> _closeForegroundWebSocket() async {
    _reconnectTimer?.cancel();
    _reconnectTimer = null;

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
        final next = await _store.nextPending();
        if (next == null) break;

        try {
          _channel!.sink.add(jsonEncode(next.payload));
          await _store.markSent(next.id!);
          await _refreshCounts();
          _log('Enviado em série e marcado como sent: ${next.sequence}');

          await Future<void>.delayed(const Duration(milliseconds: 30));
        } catch (error) {
          _log('Falha no envio serial. Mantendo como pending: $error');
          _handleSocketDisconnected('WebSocket caiu durante drain.');
          break;
        }
      }
    } finally {
      _draining = false;
    }
  }

  Future<void> _refreshCounts() async {
    final pending = await _store.countByStatus('pending');
    final sent = await _store.countByStatus('sent');

    if (!mounted) return;
    setState(() {
      _pendingCount = pending;
      _sentCount = sent;
    });
  }

  void _log(String message) {
    if (!mounted) return;
    final time = DateTime.now().toIso8601String().substring(11, 19);
    setState(() {
      _logs.insert(0, '[$time] $message');
      if (_logs.length > 100) _logs.removeLast();
    });
  }

  @override
  Widget build(BuildContext context) {
    final p = _lastPosition;

    return Scaffold(
      appBar: AppBar(
        title: const Text('GPS Background Tracker'),
      ),
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
                  _InfoRow(label: 'Lifecycle', value: _appInForeground ? 'foreground' : 'background'),
                  _InfoRow(label: 'WebSocket', value: _socketConnected ? 'conectado' : 'fechado'),
                  _InfoRow(label: 'SQLite pending', value: '$_pendingCount'),
                  _InfoRow(label: 'SQLite sent', value: '$_sentCount'),
                  _InfoRow(label: 'Reconexões', value: '$_reconnectCount'),
                ],
              ),
            ),
          ),
          const SizedBox(height: 12),
          Card(
            child: Padding(
              padding: const EdgeInsets.all(16),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text('Última posição', style: Theme.of(context).textTheme.titleLarge),
                  const SizedBox(height: 8),
                  _InfoRow(label: 'Latitude', value: p?.latitude.toString() ?? '-'),
                  _InfoRow(label: 'Longitude', value: p?.longitude.toString() ?? '-'),
                  _InfoRow(label: 'Altitude', value: p?.altitude.toString() ?? '-'),
                  _InfoRow(label: 'Precisão', value: p == null ? '-' : '${p.accuracy} m'),
                  _InfoRow(label: 'Velocidade', value: p == null ? '-' : '${p.speed} m/s'),
                  _InfoRow(label: 'Direção', value: p == null ? '-' : '${p.heading}°'),
                ],
              ),
            ),
          ),
          const SizedBox(height: 12),
          Card(
            child: Padding(
              padding: const EdgeInsets.all(16),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text('Logs', style: Theme.of(context).textTheme.titleLarge),
                  const SizedBox(height: 8),
                  ..._logs.map((line) => Text(line)),
                ],
              ),
            ),
          ),
        ],
      ),
    );
  }
}

class GpsPoint {
  GpsPoint({
    this.id,
    required this.sequence,
    required this.capturedAt,
    required this.payload,
    this.status = 'pending',
  });

  final int? id;
  final String sequence;
  final DateTime capturedAt;
  final Map<String, dynamic> payload;
  final String status;

  Map<String, dynamic> toDb() {
    return {
      'sequence': sequence,
      'captured_at': capturedAt.toUtc().toIso8601String(),
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
      capturedAt: DateTime.parse(row['captured_at'] as String),
      payload: jsonDecode(row['payload_json'] as String) as Map<String, dynamic>,
      status: row['status'] as String,
    );
  }
}

class GpsPointStore {
  Database? _db;

  Future<void> open() async {
    if (_db != null) return;

    final dbPath = await getDatabasesPath();
    final path = p.join(dbPath, 'gps_points.db');

    _db = await openDatabase(
      path,
      version: 1,
      onCreate: (db, version) async {
        await db.execute('''
          CREATE TABLE gps_points (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sequence TEXT NOT NULL UNIQUE,
            captured_at TEXT NOT NULL,
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
      },
    );
  }

  Future<void> close() async {
    await _db?.close();
    _db = null;
  }

  Future<void> insertPending(GpsPoint point) async {
    final db = _requireDb();
    await db.insert(
      'gps_points',
      point.toDb(),
      conflictAlgorithm: ConflictAlgorithm.ignore,
    );
  }

  Future<GpsPoint?> nextPending() async {
    final db = _requireDb();
    final rows = await db.query(
      'gps_points',
      where: 'status = ?',
      whereArgs: ['pending'],
      orderBy: 'id ASC',
      limit: 1,
    );

    if (rows.isEmpty) return null;
    return GpsPoint.fromDb(rows.first);
  }

  Future<void> markSent(int id) async {
    final db = _requireDb();
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
    final db = _requireDb();
    final result = await db.rawQuery(
      'SELECT COUNT(*) AS total FROM gps_points WHERE status = ?',
      [status],
    );
    return Sqflite.firstIntValue(result) ?? 0;
  }

  Database _requireDb() {
    final db = _db;
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
