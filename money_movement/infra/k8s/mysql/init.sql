CREATE USER 'money_movement_user'@'%' IDENTIFIED BY 'Auth123';

CREATE DATABASE money_movement;

GRANT ALL PRIVILEGES ON money_movement.* TO 'money_movement_user'@'%';

USE money_movement;

CREATE TABLE `wallet` (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL UNIQUE,
    wallet_type VARCHAR(255) NOT NULL,
    INDEX(user_id)
);

CREATE TABLE `account` (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    cents INT NOT NULL DEFAULT 0,
    account_type VARCHAR(255) NOT NULL,
    wallet_id INT NOT NULL,
    FOREIGN KEY (wallet_id) REFERENCES wallet(id)
);

CREATE TABLE `transaction` (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    pid VARCHAR(255) NOT NULL,
    src_user_id VARCHAR(255) NOT NULL,
    dst_user_id VARCHAR(255) NOT NULL,
    src_wallet_id INT NOT NULL,
    dst_wallet_id INT NOT NULL,
    src_account_id INT NOT NULL,
    dst_account_id INT NOT NULL,
    src_account_type VARCHAR(255) NOT NULL,
    dst_account_type VARCHAR(255) NOT NULL,
    final_dst_merchant_wallet_id INT,
    amount INT NOT NULL,
    INDEX(pid)
);