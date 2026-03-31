// Публичные модули библиотеки.
// Бинарник использует их напрямую, а позже этот же API пригодится
// интеграционным тестам и orchestration-коду вокруг sender/receiver.
pub mod app;
pub mod cli;
pub mod network;
pub mod receiver;
pub mod role;
pub mod sender;
pub mod telemetry;
