# Pilot BIM Bridge Server Extension

Серверный диспетчер задач для клиентских воркеров Pilot-BIM.

## Durable bulk v2

Публичный query protocol 1.0 сохранён; additive `IBimBridgeArtifactApi` и capabilities
включают durable uploads и native chunk downloads. Opt-in TokenHttpAdapter восстанавливает существующий `/bim-bridge` для token notebook
в том же процессе и runtime. Включается только через существующий RestApi.Enabled;
Prefix/Token/TokenEnvironmentVariable/AllowedOrigin/MaxRequestBytes сохраняются.
При ошибке HTTP binding native Custom API остаётся доступен, причина пишется в log.
За reverse proxy сохраните loopback Prefix. Для прямого внешнего доступа используйте TLS.
HTTP metadata объявляет token-http-chunks-v2; resume не требует Range.
[ADR и жизненный цикл](docs/ADR-0002-durable-artifacts.md) описывает reservations,
перезапуск Manager, missing ranges, immutable SHA-256 metadata и ограничения.
Bulk MaxArtifactBytes=8GiB, disk quota=32GiB, chunk limit=4MiB, upload TTL=30min,
result TTL=60min, reservation TTL=24h. Это отдельные настройки от query MaxResultBytes.
Сохраняйте весь каталог Results на постоянном диске и задавайте согласованные TTL
в `Pilot.BimBridge.json`. После истечения result TTL клиент не может продолжить download.

Все новые проверки используют локальные stores и fake SDK. TestHarness — тестовый
stdio процесс без сетевого listener. Он не подключается к Pilot.

## Состав

- `Pilot.BimBridge.Contracts` — публичный протокол версии `1.0`.
- `Pilot.BimBridge.ServerExtension` — серверное расширение Pilot-Server.
- `Pilot.BimBridge.ProtocolTestClient` — консольная сквозная проверка через `HttpPilotClient`.
- `Pilot.BimBridge.UnitTests` — проверки состояний, очереди, long polling и lease.
- `Pilot.BimBridge.IntegrationTests` — проверки файловой передачи результатов.

В `ServerExtension` планирование, lease и task lifecycle находятся в
`BimBridgeRuntime.cs`; методы прежнего upload/download API — в
`LegacyArtifactRuntime.cs`; resumable artifact v2 — в `ArtifactRuntime.cs`.
`ArtifactTaskState.cs` сохраняет состояние и профиль попыток на диске.
`DiagnosticTransport.cs` ограничивает размер необязательной диагностики 64 KiB,
проверяет content type и SHA-256, но не разбирает схему worker. Envelope хранится
вместе с upload/task state и удаляется по действующему TTL задачи и результата.
Новый worker отправляет его вместе с прежним `SpatialPerformance` для переходной
совместимости. Manager сохраняет старое поле без анализа SDK timings; PSIMpy
проверяет и интерпретирует известную схему. Поле статистики
`AverageProcessingWallMsByWorker` сохранено в контракте, но больше не вычисляется:
производительность обработки рассчитывает PSIMpy по профилям задач.
`TokenHttpAdapter.cs` лишь адаптирует существующий token endpoint к этому runtime.
Новые поля telemetry в `Contracts` добавлены без смены занятых protobuf tags;
отсутствующие длительности остаются nullable. Manager измеряет свои интервалы
`Stopwatch`, а UTC события использует для timeline. Метрики worker передаются с
durable artifact и не выводятся из разницы глобальных счётчиков Manager.

Серверные проекты имеют целевую платформу `.NET Standard 2.0` и используют
`Ascon.Pilot.ServerExtensions.SDK` версии `26.10.0`.

## Сборка

Подготовка согласованных версий, установка и rollback: [release-readiness.md](docs/release-readiness.md).

```powershell
dotnet build Pilot.BimBridge.sln --configuration Release
```

По умолчанию результаты сборки записываются в:

```text
C:\Users\roman\Desktop\BWM build
```

Путь можно переопределить свойством:

```powershell
dotnet build Pilot.BimBridge.sln -p:BimBridgeBuildRoot=D:\Build
```

## Конфигурация

Перед установкой необходимо изменить `Results.Directory` в
`Pilot.BimBridge.json`. Каталог результатов должен:

- находиться вне каталога исполняемых файлов Pilot-Server;
- быть доступен учётной записи службы Pilot-Server;
- иметь достаточно свободного места;
- включаться в резервное копирование только при необходимости.

Изменение конфигурации требует перезапуска Pilot-Server.

## Установка

1. Собрать проект `Pilot.BimBridge.ServerExtension` в конфигурации `Release`.
2. Скопировать содержимое каталога
   `BWM build\Pilot.BimBridge.ServerExtension\Release\netstandard2.0`
   в отдельный каталог расширения внутри `Development` Pilot-Server.
3. Настроить `Pilot.BimBridge.json`.
4. Перезапустить Pilot-Server.
5. Проверить `GetHealthAsync` с помощью `Pilot.BimBridge.ProtocolTestClient`.

## Локальные тесты

Тестовые данные создаются в:

```text
C:\Users\roman\Desktop\BWM tests
```

Запуск:

```powershell
$env:BIM_BRIDGE_TEST_ROOT = 'C:\Users\roman\Desktop\BWM tests'
dotnet test Pilot.BimBridge.sln
```

Проверка загрузки MEF и реального клиент-серверного транспорта возможна только
на стенде с Pilot-Server 26.10.

## Parallel shard pipeline

Queue/execution/delivery semantics, additive contracts, metrics and regression tests: [ADR-0003](docs/ADR-0003-parallel-pipeline.md). The existing token binding exposes `GET /statistics`; native clients use `IBimBridgeDiagnosticsApi.GetStatisticsAsync`.
