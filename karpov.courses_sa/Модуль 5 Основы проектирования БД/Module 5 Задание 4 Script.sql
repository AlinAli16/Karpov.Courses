-- -----------------------------------------------------------------------------
-- Исходная (ненормализованная) таблица UserSessions (для наглядности)
-- -----------------------------------------------------------------------------

DROP TABLE IF EXISTS UserSessions_Original;

CREATE TABLE UserSessions_Original (
    user_id INT,
    name VARCHAR(255),
    email VARCHAR(255),
    status VARCHAR(50),
    device_name VARCHAR(255),
    token VARCHAR(255),
    issued_at DATETIME,
    expires_at DATETIME,
    role_name VARCHAR(50),
    permission_resource VARCHAR(255),
    permission_action VARCHAR(50)
);

-- Функциональные зависимости в исходной таблице:
-- token -> user_id, device_name, issued_at, expires_at, role_name
-- user_id -> name, email, status
-- role_name -> permission_resource, permission_action

-- -----------------------------------------------------------------------------
-- Этап 1: 1NF (Первая нормальная форма)
--
-- Убираем повторяющиеся группы (многозначные атрибуты). В данном примере исходная
-- таблица уже находится в 1NF, так как каждое поле содержит только одно значение.
-- -----------------------------------------------------------------------------

-- Переход к 1NF в данном случае не требует изменений структуры таблицы.
-- (UserSessions_Original соответствует 1NF)

-- -----------------------------------------------------------------------------
-- Этап 2: 2NF (Вторая нормальная форма)
--
-- Устраняем частичные зависимости от составного ключа.
-- В исходной таблице UserSessions_Original, если бы  'token' не был уникальным и
-- первичным ключом, а вместо этого первичным ключом был бы составной ключ
-- (user_id, device_name), то атрибуты 'name', 'email', 'status' имели бы частичную
-- зависимость от 'user_id'. Для приведения к 2NF нужно выделить таблицу Users.
-- -----------------------------------------------------------------------------

-- Создаем таблицу Users для устранения частичной зависимости.

DROP TABLE IF EXISTS Users;

CREATE TABLE Users (
    user_id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    status VARCHAR(50) NOT NULL
);

-- Аномалии, решенные при переходе к 2NF (предположим, что token не уникален
-- и первичный ключ - (user_id, device_name)):
--   - Аномалия обновления: Если нужно изменить email пользователя, нужно было бы
--     обновлять все записи в UserSessions, где он появляется. Теперь достаточно
--     изменить одну запись в таблице Users.
--   - Аномалия вставки: Нельзя добавить нового пользователя, не создав сессию.
--     Теперь можно добавить пользователя в Users без необходимости создавать
--     сессию.

-- -----------------------------------------------------------------------------
-- Этап 3: 3NF (Третья нормальная форма)
--
-- Устраняем транзитивные зависимости.
--
-- Проблема: В таблице UserSessions есть транзитивная зависимость:
-- token -> role_name -> permission_resource, permission_action
-- Это означает, что 'permission_resource' и 'permission_action' зависят от
-- 'token' через 'role_name', а не напрямую.
--
-- Решение: Создаем таблицы Roles и Permissions, чтобы устранить транзитивную зависимость.
-- -----------------------------------------------------------------------------

-- Создаем таблицы Roles и Permissions.

DROP TABLE IF EXISTS Roles;

CREATE TABLE Roles (
    role_name VARCHAR(50) PRIMARY KEY
);

DROP TABLE IF EXISTS Permissions;

CREATE TABLE Permissions (
    role_name VARCHAR(50) NOT NULL,
    permission_resource VARCHAR(255) NOT NULL,
    permission_action VARCHAR(50) NOT NULL,
    PRIMARY KEY (role_name, permission_resource, permission_action),
    FOREIGN KEY (role_name) REFERENCES Roles(role_name)
);

DROP TABLE IF EXISTS Devices;

CREATE TABLE Devices (
    device_name VARCHAR(255) PRIMARY KEY
);

-- Изменяем таблицу UserSessions, чтобы она зависела только от PK и FK.

DROP TABLE IF EXISTS UserSessions;

CREATE TABLE UserSessions (
    token VARCHAR(255) PRIMARY KEY,
    user_id INT NOT NULL,
    device_name VARCHAR(255) NOT NULL,
    issued_at DATETIME NOT NULL,
    expires_at DATETIME NOT NULL,
    role_name VARCHAR(50) NOT NULL,
    FOREIGN KEY (user_id) REFERENCES Users(user_id),
    FOREIGN KEY (device_name) REFERENCES Devices(device_name),
    FOREIGN KEY (role_name) REFERENCES Roles(role_name)
);

-- Аномалии, решенные при переходе к 3NF:
--   - Аномалия обновления: Если нужно изменить разрешение для роли, нужно было бы
--     обновлять все записи в UserSessions, где эта роль используется. Теперь
--     достаточно изменить одну запись в таблице Permissions.
--   - Аномалия вставки: Нельзя добавить новую роль и ее разрешения, не создав
--     сессию. Теперь можно добавить роль и разрешения в таблицы Roles и
--     Permissions без необходимости создавать сессию.
--   - Аномалия удаления: Если удалить все сессии с определенной ролью, информация
--     о разрешениях этой роли будет потеряна. Теперь информация о ролях и
--     разрешениях хранится отдельно и не теряется при удалении сессий.

-- -----------------------------------------------------------------------------
-- Заполнение таблиц данными из примера
-- -----------------------------------------------------------------------------

INSERT INTO Users (user_id, name, email, status) VALUES
(1, 'John Doe', 'john@example.com', 'active'),
(2, 'Jane Smith', 'jane@example.com', 'blocked'),
(3, 'Jane Smith', 'jane@example.com', 'blocked');

INSERT INTO Devices (device_name) VALUES
('iPhone 13'),
('MacBook Pro'),
('Galaxy S21');

INSERT INTO Roles (role_name) VALUES
('admin'),
('user');

INSERT INTO Permissions (role_name, permission_resource, permission_action) VALUES
('admin', 'account/settings', 'edit'),
('user', 'profile/view', 'read');

INSERT INTO UserSessions (token, user_id, device_name, issued_at, expires_at, role_name) VALUES
('token_abc123', 1, 'iPhone 13', '2025-07-15 08:00', '2025-07-15 10:00', 'admin'),
('token_xyz789', 1, 'MacBook Pro', '2025-07-15 09:00', '2025-07-15 11:00', 'admin'),
('token_def456', 2, 'Galaxy S21', '2025-07-14 07:45', '2025-07-14 09:45', 'user'),
('token_hij111', 3, 'Galaxy S21', '2025-07-14 10:00', '2025-07-14 12:00', 'user');

-- -----------------------------------------------------------------------------
-- Все функциональные зависимости (после нормализации):
-- -----------------------------------------------------------------------------
-- Users:
--   user_id -> name, email, status
-- Roles:
--   role_name -> (nothing - только первичный ключ)
-- Permissions:
--   (role_name, permission_resource) -> permission_action
-- UserSessions:
--   token -> user_id, device_name, issued_at, expires_at, role_name
-- Devices:
--   device_name -> (nothing - только первичный ключ)
