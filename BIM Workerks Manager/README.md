# Pilot BIM Bridge Server Extension

Серверный диспетчер задач для клиентских воркеров Pilot-BIM.

## Состав

- `Pilot.BimBridge.Contracts` — публичный протокол версии `1.0`.
- `Pilot.BimBridge.ServerExtension` — серверное расширение Pilot-Server.
- `Pilot.BimBridge.ProtocolTestClient` — консольная сквозная проверка через `HttpPilotClient`.
- `Pilot.BimBridge.UnitTests` — проверки состояний, очереди, long polling и lease.
- `Pilot.BimBridge.IntegrationTests` — проверки файловой передачи результатов.

Серверные проекты имеют целевую платформу `.NET Standard 2.0` и используют
`Ascon.Pilot.ServerExtensions.SDK` версии `26.10.0`.

## Сборка

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
