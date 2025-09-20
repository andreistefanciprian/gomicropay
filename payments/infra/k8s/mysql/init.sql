CREATE USER 'payments_user'@'%' IDENTIFIED BY 'Auth123';

CREATE DATABASE payments;

GRANT ALL PRIVILEGES ON payments.* TO 'payments_user'@'%';

USE payments;

CREATE TABLE `wallet` (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email_address VARCHAR(255) NOT NULL UNIQUE,
    wallet_type VARCHAR(255) NOT NULL,
    INDEX(email_address)
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
    src_email_address VARCHAR(255) NOT NULL,
    dst_email_address VARCHAR(255) NOT NULL,
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