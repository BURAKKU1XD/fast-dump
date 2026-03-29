// Публичные модули библиотеки.
// Бинарник использует их напрямую, а позже этот же API пригодится
// интеграционным тестам и orchestration-коду вокруг sender/receiver.
pub mod cli;
pub mod network;
pub mod role;
