package db

import (
	"context"
	"database/sql"

	"github.com/XSAM/otelsql"
	proto "github.com/andreistefanciprian/gomicropay/money_movement/proto"
	"github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	selectTransactionQuery   = "SELECT pid, src_email_address, dst_email_address, src_wallet_id, dst_wallet_id, src_account_id, dst_account_id, src_account_type, dst_account_type, final_dst_merchant_wallet_id, amount FROM transaction WHERE pid = ?"
	insertTransactionQuery   = "INSERT INTO transaction (pid, src_email_address, dst_email_address, src_wallet_id, dst_wallet_id, src_account_id, dst_account_id, src_account_type, dst_account_type, final_dst_merchant_wallet_id, amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	createWalletQuery        = "INSERT INTO wallet (email_address, wallet_type) VALUES (?, ?)"
	createAccountQuery       = "INSERT INTO account (cents, account_type, wallet_id) VALUES (?, ?, ?)"
	selectWalletByEmailQuery = "SELECT id, email_address, wallet_type FROM wallet WHERE email_address = ?"
	selectWalletByIDQuery    = "SELECT id, email_address, wallet_type FROM wallet WHERE id = ?"
	selectAccountQuery       = "SELECT id, cents, account_type, wallet_id FROM account WHERE wallet_id = ? AND account_type = ?"
	updateAccountCentsQuery  = "UPDATE account SET cents = ? WHERE id = ?"
)

type Wallet struct {
	ID           int32
	EmailAddress string
	WalletType   string
}

type Account struct {
	ID          int32
	Cents       int64
	AccountType string
	WalletID    int32
}

type Transaction struct {
	ID                       int32
	Pid                      string
	SrcEmailAddress          string
	DstEmailAddress          string
	SrcAccountWalletID       int32
	DstAccountWalletID       int32
	SrcAccountID             int32
	DstAccountID             int32
	SrcAccountType           string
	DstAccountType           string
	FinalDstMerchantWalletID int32
	Amount                   int64
}

type MoneyMovementRepository interface {
	FetchWallet(ctx context.Context, tx *sql.Tx, emailAddress string) (Wallet, error)
	FetchWalletWithWalletID(ctx context.Context, tx *sql.Tx, walletID int32) (Wallet, error)
	FetchAccount(ctx context.Context, tx *sql.Tx, walletID int32, accountType string) (Account, error)
	Transfer(ctx context.Context, tx *sql.Tx, srcAccount Account, dstAccount Account, amount int64) error
	CreateTransaction(ctx context.Context, tx *sql.Tx, pid string, srcAccount Account, dstAccount Account, srcWallet Wallet, dstWallet Wallet, finalDstWallet Wallet, amount int64) error
	CreateWallet(ctx context.Context, tx *sql.Tx, emailAddress string, walletType proto.WalletType) (int64, error)
	CreateAccount(ctx context.Context, tx *sql.Tx, amount int64, accountType string, walletId int64) error
	FetchTransaction(ctx context.Context, tx *sql.Tx, pid string) (Transaction, error)
	Close() error
	BeginTransaction(ctx context.Context) (*sql.Tx, error)
	CommitTransaction(tx *sql.Tx) error
}

type MySqlDb struct {
	DB     *sql.DB
	logger *logrus.Logger
	tracer trace.Tracer
}

// NewMysqlDb creates a new MySqlDb instance with an instrumented DB connection
func NewMysqlDb(dbName string, dsn string, tracerProvider trace.TracerProvider, logger *logrus.Logger) (*MySqlDb, error) {
	db, err := otelsql.Open(dbName, dsn, otelsql.WithTracerProvider(tracerProvider))
	if err != nil {
		return &MySqlDb{}, err
	}
	// Ping db
	if err = db.Ping(); err != nil {
		return &MySqlDb{}, err
	} else {
		logger.Info("Database connection established")
	}
	tracer := tracerProvider.Tracer("db")
	return &MySqlDb{DB: db, logger: logger, tracer: tracer}, nil
}

// Close the database connection
func (m *MySqlDb) Close() error {
	if err := m.DB.Close(); err != nil {
		m.logger.Error("Error closing database connection: ", err)
		return err
	}
	m.logger.Info("Database connection closed")
	return nil
}

func (m *MySqlDb) BeginTransaction(ctx context.Context) (*sql.Tx, error) {
	// Begin the transaction
	tx, err := m.DB.BeginTx(ctx, nil)
	if err != nil {
		m.logger.Infof("BeginTx failed: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	return tx, nil
}

func (m *MySqlDb) CommitTransaction(tx *sql.Tx) error {
	// Commit the transaction
	err := tx.Commit()
	if err != nil {
		m.logger.Infof("CommitTx failed: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			m.logger.Errorf("CommitTx failed: could not rollback after commit error: %v", rollbackErr)
			return status.Error(codes.Internal, rollbackErr.Error())
		}
		return status.Error(codes.Internal, err.Error())
	}
	return nil
}

func (m *MySqlDb) FetchWallet(ctx context.Context, tx *sql.Tx, emailAddress string) (Wallet, error) {
	ctx, span := m.tracer.Start(ctx, "FetchWallet")
	defer span.End()

	var w Wallet

	stmt, err := tx.PrepareContext(ctx, selectWalletByEmailQuery)
	if err != nil {
		return w, status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	err = stmt.QueryRowContext(ctx, emailAddress).Scan(&w.ID, &w.EmailAddress, &w.WalletType)
	if err != nil {
		if err == sql.ErrNoRows {
			return w, status.Error(codes.InvalidArgument, "wallet not found")
		}
		return w, status.Error(codes.Internal, err.Error())
	}

	return w, nil
}

func (m *MySqlDb) FetchWalletWithWalletID(ctx context.Context, tx *sql.Tx, walletID int32) (Wallet, error) {
	ctx, span := m.tracer.Start(ctx, "FetchWalletWithWalletID")
	defer span.End()
	var w Wallet

	stmt, err := tx.PrepareContext(ctx, selectWalletByIDQuery)
	if err != nil {
		return w, status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	err = stmt.QueryRowContext(ctx, walletID).Scan(&w.ID, &w.EmailAddress, &w.WalletType)
	if err != nil {
		if err == sql.ErrNoRows {
			return w, status.Error(codes.InvalidArgument, "wallet not found")
		}
		return w, status.Error(codes.Internal, err.Error())
	}

	return w, nil
}

func (m *MySqlDb) FetchAccount(ctx context.Context, tx *sql.Tx, walletID int32, accountType string) (Account, error) {
	ctx, span := m.tracer.Start(ctx, "FetchAccount")
	defer span.End()
	var a Account

	stmt, err := tx.PrepareContext(ctx, selectAccountQuery)
	if err != nil {
		return a, status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	err = stmt.QueryRowContext(ctx, walletID, accountType).Scan(&a.ID, &a.Cents, &a.AccountType, &a.WalletID)
	if err != nil {
		if err == sql.ErrNoRows {
			return a, status.Error(codes.InvalidArgument, "account not found")
		}
		return a, status.Error(codes.Internal, err.Error())
	}

	return a, nil
}

func (m *MySqlDb) Transfer(ctx context.Context, tx *sql.Tx, srcAccount Account, dstAccount Account, amount int64) error {
	ctx, span := m.tracer.Start(ctx, "Transfer")
	defer span.End()
	if srcAccount.Cents < amount {
		return status.Error(codes.FailedPrecondition, "insufficient funds")
	}

	// Deduct from source account
	newSrcCents := srcAccount.Cents - amount
	stmt, err := tx.PrepareContext(ctx, updateAccountCentsQuery)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx, newSrcCents, srcAccount.ID)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	// Add to destination account
	newDstCents := dstAccount.Cents + amount
	stmt, err = tx.PrepareContext(ctx, updateAccountCentsQuery)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx, newDstCents, dstAccount.ID)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	return nil
}

func (m *MySqlDb) CreateTransaction(ctx context.Context, tx *sql.Tx, pid string, srcAccount Account, dstAccount Account, srcWallet Wallet, dstWallet Wallet, finalDstWallet Wallet, amount int64) error {
	ctx, span := m.tracer.Start(ctx, "CreateTransaction")
	defer span.End()

	stmt, err := tx.PrepareContext(ctx, insertTransactionQuery)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx, pid, srcWallet.EmailAddress, dstWallet.EmailAddress, srcWallet.ID, dstWallet.ID, srcAccount.ID, dstAccount.ID, srcAccount.AccountType, dstAccount.AccountType, finalDstWallet.ID, amount)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	return nil
}

func (m *MySqlDb) CreateWallet(ctx context.Context, tx *sql.Tx, emailAddress string, walletType proto.WalletType) (int64, error) {
	ctx, span := m.tracer.Start(ctx, "CreateWallet")
	defer span.End()

	stmt, err := tx.PrepareContext(ctx, createWalletQuery)
	if err != nil {
		return -1, status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	// Convert enum to string before insert
	var walletTypeStr string
	switch walletType {
	case proto.WalletType_CUSTOMER:
		walletTypeStr = "CUSTOMER"
	case proto.WalletType_MERCHANT:
		walletTypeStr = "MERCHANT"
	default:
		walletTypeStr = "UNKNOWN"
	}

	result, err := stmt.ExecContext(ctx, emailAddress, walletTypeStr)
	if err != nil {
		if mysqlErr, ok := err.(*mysql.MySQLError); ok && mysqlErr.Number == 1062 {
			return -1, status.Error(codes.AlreadyExists, "wallet already exists")
		}
		return -1, status.Error(codes.Internal, err.Error())
	}

	id, err := result.LastInsertId()
	if err != nil {
		return -1, status.Error(codes.Internal, err.Error())
	}

	return id, nil
}

func (m *MySqlDb) CreateAccount(ctx context.Context, tx *sql.Tx, amount int64, accountType string, walletId int64) error {
	ctx, span := m.tracer.Start(ctx, "CreateAccount")
	defer span.End()

	stmt, err := tx.PrepareContext(ctx, createAccountQuery)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx, amount, accountType, walletId)
	if err != nil {
		if mysqlErr, ok := err.(*mysql.MySQLError); ok && mysqlErr.Number == 1062 {
			return status.Error(codes.AlreadyExists, "account already exists")
		}
		return status.Error(codes.Internal, err.Error())
	}

	return nil
}

func (m *MySqlDb) FetchTransaction(ctx context.Context, tx *sql.Tx, pid string) (Transaction, error) {
	ctx, span := m.tracer.Start(ctx, "FetchTransaction")
	defer span.End()
	var t Transaction

	stmt, err := tx.PrepareContext(ctx, selectTransactionQuery)
	if err != nil {
		return t, status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	err = stmt.QueryRowContext(ctx, pid).Scan(&t.Pid, &t.SrcEmailAddress, &t.DstEmailAddress, &t.SrcAccountWalletID, &t.DstAccountWalletID, &t.SrcAccountID, &t.DstAccountID, &t.SrcAccountType, &t.DstAccountType, &t.FinalDstMerchantWalletID, &t.Amount)
	if err != nil {
		if err == sql.ErrNoRows {
			return t, status.Error(codes.InvalidArgument, "transaction not found")
		}
		return t, status.Error(codes.Internal, err.Error())
	}

	return t, nil
}
