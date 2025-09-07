package mm

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/IBM/sarama"
	"github.com/andreistefanciprian/gomicropay/money_movement/internal/producer"
	pb "github.com/andreistefanciprian/gomicropay/money_movement/proto"
	"github.com/go-sql-driver/mysql"
	"github.com/gogo/status"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/emptypb"
)

var logLevel = getLogLevel()

func getLogLevel() string {
	lvl := os.Getenv("LOG_LEVEL")
	if lvl == "" {
		return "INFO"
	}
	return lvl
}

func logInfo(format string, v ...interface{}) {
	if logLevel == "INFO" || logLevel == "DEBUG" {
		log.Printf("INFO: "+format, v...)
	}
}

func logDebug(format string, v ...interface{}) {
	if logLevel == "DEBUG" {
		log.Printf("DEBUG: "+format, v...)
	}
}

const (
	selectTransactionQuery = "SELECT pid, src_email_address, dst_email_address, src_wallet_id, dst_wallet_id, src_account_id, dst_account_id, src_account_type, dst_account_type, final_dst_merchant_wallet_id, amount FROM transaction WHERE pid = ?"
	insertTransactionQuery = "INSERT INTO transaction (pid, src_email_address, dst_email_address, src_wallet_id, dst_wallet_id, src_account_id, dst_account_id, src_account_type, dst_account_type, final_dst_merchant_wallet_id, amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	createWalletQuery      = "INSERT INTO wallet (email_address, wallet_type) VALUES (?, ?)"
	createAccountQuery     = "INSERT INTO account (cents, account_type, wallet_id) VALUES (?, ?, ?)"
)

type Implementation struct {
	db       *sql.DB
	producer sarama.SyncProducer
	pb.UnimplementedMoneyMovementServiceServer
	tracer trace.Tracer
}

func NewMoneyMovementImplementation(db *sql.DB, producer sarama.SyncProducer, tracer trace.Tracer) *Implementation {
	return &Implementation{
		db:       db,
		producer: producer,
		tracer:   tracer,
	}
}

func (i *Implementation) Authorize(ctx context.Context, authorizePayload *pb.AuthorizePayload) (*pb.AuthorizeResponse, error) {
	// Start a new span for tracing
	ctx, span := i.tracer.Start(ctx, "Authorize")
	defer span.End()

	span.SetAttributes(
		attribute.String("customer_email", authorizePayload.GetCustomerEmailAddress()),
		attribute.String("customer_email", authorizePayload.GetMerchantEmailAddress()),
		attribute.String("customer_email", strconv.Itoa(int(authorizePayload.GetCents()))),
	)

	logInfo("Authorize called with payload: %+v", authorizePayload)

	if authorizePayload.GetCurrency() != "USD" {
		logInfo("Authorize failed: only accepts USD")
		return nil, status.Error(codes.InvalidArgument, "only accepts USD")
	}
	// Begin transaction
	tx, err := i.db.Begin()
	if err != nil {
		logInfo("Authorize failed: could not begin transaction: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	merchantWallet, err := fetchWallet(ctx, tx, authorizePayload.MerchantEmailAddress)
	if err != nil {
		logInfo("Authorize failed: could not fetch merchant wallet: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("Authorize failed: could not rollback after merchant wallet error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	customerWallet, err := fetchWallet(ctx, tx, authorizePayload.CustomerEmailAddress)
	if err != nil {
		logInfo("Authorize failed: could not fetch customer wallet: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("Authorize failed: could not rollback after customer wallet error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	srcAccount, err := fetchAccount(ctx, tx, customerWallet.ID, "DEFAULT")
	if err != nil {
		logInfo("Authorize failed: could not fetch src account: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("Authorize failed: could not rollback after src account error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	dstAccount, err := fetchAccount(ctx, tx, customerWallet.ID, "PAYMENT")
	if err != nil {
		logInfo("Authorize failed: could not fetch dst account: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("Authorize failed: could not rollback after dst account error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	logDebug("Authorize: transferring %d cents from srcAccount %d to dstAccount %d", authorizePayload.Cents, srcAccount.ID, dstAccount.ID)
	err = transfer(ctx, tx, srcAccount, dstAccount, authorizePayload.Cents)
	if err != nil {
		logInfo("Authorize failed: transfer error: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("Authorize failed: could not rollback after transfer error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	pid := uuid.New().String()
	logDebug("Authorize: creating transaction with pid %s", pid)
	err = createTransaction(ctx, tx, pid, srcAccount, dstAccount, customerWallet, customerWallet, merchantWallet, authorizePayload.Cents)
	if err != nil {
		logInfo("Authorize failed: createTransaction error: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("Authorize failed: could not rollback after createTransaction error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	// End transaction
	err = tx.Commit()
	if err != nil {
		logInfo("Authorize failed: commit error: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	logInfo("Authorize succeeded: pid=%s", pid)
	return &pb.AuthorizeResponse{Pid: pid}, nil
}

func (i *Implementation) Capture(ctx context.Context, capturePayload *pb.CapturePayload) (*emptypb.Empty, error) {
	ctx, span := i.tracer.Start(ctx, "Capture")
	defer span.End()

	span.SetAttributes(
		attribute.String("pid", capturePayload.GetPid()),
	)
	logInfo("Capture called with payload: %+v", capturePayload)
	// Begin the transaction
	tx, err := i.db.Begin()
	if err != nil {
		logInfo("Capture failed: could not begin transaction: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	authorizeTransaction, err := fetchTransaction(ctx, tx, capturePayload.Pid)
	if err != nil {
		logInfo("Capture failed: could not fetch transaction: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("Capture failed: could not rollback after fetchTransaction error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	srcAccount, err := fetchAccount(ctx, tx, authorizeTransaction.dstAccountWalletID, "PAYMENT")
	if err != nil {
		logInfo("Capture failed: could not fetch src account: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("Capture failed: could not rollback after src account error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	dstMerchantAccount, err := fetchAccount(ctx, tx, authorizeTransaction.finalDstMerchantWalletID, "INCOMING")
	if err != nil {
		logInfo("Capture failed: could not fetch dst merchant account: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("Capture failed: could not rollback after dst merchant account error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	logDebug("Capture: transferring %d cents from srcAccount %d to dstMerchantAccount %d", authorizeTransaction.amount, srcAccount.ID, dstMerchantAccount.ID)
	err = transfer(ctx, tx, srcAccount, dstMerchantAccount, authorizeTransaction.amount)
	if err != nil {
		logInfo("Capture failed: transfer error: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("Capture failed: could not rollback after transfer error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	customerWallet, err := fetchWallet(ctx, tx, authorizeTransaction.srcEmailAddress)
	if err != nil {
		logInfo("Capture failed: could not fetch customer wallet: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("Capture failed: could not rollback after customer wallet error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	merchantWallet, err := fetchWalletWithWalletID(ctx, tx, authorizeTransaction.finalDstMerchantWalletID)
	if err != nil {
		logInfo("Capture failed: could not fetch merchant wallet: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("Capture failed: could not rollback after merchant wallet error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	err = createTransaction(ctx, tx, authorizeTransaction.pid, srcAccount, dstMerchantAccount, customerWallet, merchantWallet, merchantWallet, authorizeTransaction.amount)
	if err != nil {
		logInfo("Capture failed: createTransaction error: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("Capture failed: could not rollback after createTransaction error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	// Commit tx
	err = tx.Commit()
	if err != nil {
		logInfo("Capture failed: commit error: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("Capture failed: could not rollback after commit error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	logInfo("Transaction succeeded: pid=%s", authorizeTransaction.pid)

	// Send Kafka message
	logInfo("Sending Kafka messages for transaction: pid=%s", authorizeTransaction.pid)
	p := producer.NewKafkaProducer(i.producer, i.tracer)
	p.SendCaptureMessage(ctx, authorizeTransaction.pid, authorizeTransaction.srcEmailAddress, authorizeTransaction.amount)

	return &emptypb.Empty{}, nil
}

func (i *Implementation) CreateAccount(ctx context.Context, createAccountPayload *pb.CreateAccountPayload) (*emptypb.Empty, error) {
	ctx, span := i.tracer.Start(ctx, "CreateAccount")
	defer span.End()

	span.SetAttributes(
		attribute.String("email_address", createAccountPayload.GetEmailAddress()),
		attribute.String("wallet_type", pb.WalletType.String(createAccountPayload.GetWalletType())),
	)
	logInfo("CreateAccount called with payload: %+v", createAccountPayload)

	// Begin the transaction
	tx, err := i.db.Begin()
	if err != nil {
		logInfo("CreateAccount failed: could not begin transaction: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Create wallet
	walletId, err := createWallet(ctx, tx, createAccountPayload.EmailAddress, createAccountPayload.WalletType)
	if err != nil {
		logInfo("Create Account failed: createWallet error: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("Create Account failed: could not rollback after createWallet error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	//  Determine account type
	if createAccountPayload.WalletType == pb.WalletType_CUSTOMER {
		//  Create Customer DEFAULT Account
		err = createAccount(ctx, tx, createAccountPayload.InitialBalanceCents, "DEFAULT", int64(walletId))
		if err != nil {
			logInfo("Create Account failed: createAccount error: %v", err)
			rollbackErr := tx.Rollback()
			if rollbackErr != nil {
				fmt.Printf("Create Account failed: could not rollback after createAccount error: %v\n", rollbackErr)
				return nil, status.Error(codes.Internal, rollbackErr.Error())
			}
			return nil, err
		}
		//  Create Customer PAYMENT Account
		err = createAccount(ctx, tx, 0, "PAYMENT", int64(walletId))
		if err != nil {
			logInfo("Create Account failed: createAccount error: %v", err)
			rollbackErr := tx.Rollback()
			if rollbackErr != nil {
				fmt.Printf("Create Account failed: could not rollback after createAccount error: %v\n", rollbackErr)
				return nil, status.Error(codes.Internal, rollbackErr.Error())
			}
			return nil, err
		}

	} else {
		//  Create Merchant INCOMING Account
		err = createAccount(ctx, tx, 0, "INCOMING", int64(walletId))
		if err != nil {
			logInfo("Create Account failed: createAccount error: %v", err)
			rollbackErr := tx.Rollback()
			if rollbackErr != nil {
				fmt.Printf("Create Account failed: could not rollback after createAccount error: %v\n", rollbackErr)
				return nil, status.Error(codes.Internal, rollbackErr.Error())
			}
			return nil, err
		}
	}

	// Commit tx
	err = tx.Commit()
	if err != nil {
		logInfo("Create Account failed: commit error: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("Create Account failed: could not rollback after commit error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	logInfo("CreateAccount succeeded: %s %s", pb.WalletType.String(createAccountPayload.WalletType), createAccountPayload.EmailAddress)
	return &emptypb.Empty{}, nil
}

func (i *Implementation) CheckBalance(ctx context.Context, checkBalancePayload *pb.CheckBalancePayload) (*pb.CheckBalanceResponse, error) {
	ctx, span := i.tracer.Start(ctx, "CheckBalance")
	defer span.End()

	span.SetAttributes(
		attribute.String("email_address", checkBalancePayload.GetEmailAddress()),
	)
	logInfo("CheckBalance called with payload: %+v", checkBalancePayload)

	// Begin the transaction
	tx, err := i.db.Begin()
	if err != nil {
		logInfo("CheckBalance failed: could not begin transaction: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	// get wallet based on email_address
	wallet, err := fetchWallet(ctx, tx, checkBalancePayload.EmailAddress)
	if err != nil {
		logInfo("CheckBalance failed: could not fetch wallet: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("CheckBalance failed: could not rollback after fetchWallet error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}
	logDebug("Fetched wallet: %+v", wallet)

	// determine account type based on wallet_type
	var accountType string
	if wallet.walletType == "CUSTOMER" {
		accountType = "DEFAULT"
	} else {
		accountType = "INCOMING"
	}

	// get account data based on wallet_id
	account, err := fetchAccount(ctx, tx, wallet.ID, accountType)
	if err != nil {
		logInfo("CheckBalance failed: could not fetch account: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("CheckBalance failed: could not rollback after fetchAccount error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}
	logDebug("Fetched account: %+v", account)

	// Commit tx
	err = tx.Commit()
	if err != nil {
		logInfo("CheckBalance failed: commit error: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("CheckBalance failed: could not rollback after commit error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	logInfo("CheckBalance succeeded: emailAddress=%s, balanceCents=%d", checkBalancePayload.EmailAddress, account.cents)
	return &pb.CheckBalanceResponse{
		BalanceCents: account.cents,
	}, nil
}

func fetchWallet(ctx context.Context, tx *sql.Tx, emailAddress string) (wallet, error) {
	ctx, span := trace.SpanFromContext(ctx).TracerProvider().Tracer("db").Start(ctx, "fetchWallet")
	defer span.End()
	var w wallet

	stmt, err := tx.PrepareContext(ctx, "SELECT id, email_address, wallet_type FROM wallet WHERE email_address = ?")
	if err != nil {
		return w, status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	err = stmt.QueryRowContext(ctx, emailAddress).Scan(&w.ID, &w.emailAddress, &w.walletType)
	if err != nil {
		if err == sql.ErrNoRows {
			return w, status.Error(codes.InvalidArgument, "wallet not found")
		}
		return w, status.Error(codes.Internal, err.Error())
	}

	return w, nil
}

func fetchWalletWithWalletID(ctx context.Context, tx *sql.Tx, walletID int32) (wallet, error) {
	ctx, span := trace.SpanFromContext(ctx).TracerProvider().Tracer("db").Start(ctx, "fetchWalletWithWalletID")
	defer span.End()
	var w wallet

	stmt, err := tx.PrepareContext(ctx, "SELECT id, email_address, wallet_type FROM wallet WHERE id = ?")
	if err != nil {
		return w, status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	err = stmt.QueryRowContext(ctx, walletID).Scan(&w.ID, &w.emailAddress, &w.walletType)
	if err != nil {
		if err == sql.ErrNoRows {
			return w, status.Error(codes.InvalidArgument, "wallet not found")
		}
		return w, status.Error(codes.Internal, err.Error())
	}

	return w, nil
}

func fetchAccount(ctx context.Context, tx *sql.Tx, walletID int32, accountType string) (account, error) {
	ctx, span := trace.SpanFromContext(ctx).TracerProvider().Tracer("db").Start(ctx, "fetchAccount")
	defer span.End()
	var a account

	stmt, err := tx.PrepareContext(ctx, "SELECT id, cents, account_type, wallet_id FROM account WHERE wallet_id = ? AND account_type = ?")
	if err != nil {
		return a, status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	err = stmt.QueryRowContext(ctx, walletID, accountType).Scan(&a.ID, &a.cents, &a.accountType, &a.walletID)
	if err != nil {
		if err == sql.ErrNoRows {
			return a, status.Error(codes.InvalidArgument, "account not found")
		}
		return a, status.Error(codes.Internal, err.Error())
	}

	return a, nil
}

func transfer(ctx context.Context, tx *sql.Tx, srcAccount account, dstAccount account, amount int64) error {
	ctx, span := trace.SpanFromContext(ctx).TracerProvider().Tracer("db").Start(ctx, "transfer")
	defer span.End()
	if srcAccount.cents < amount {
		return status.Error(codes.FailedPrecondition, "insufficient funds")
	}

	// Deduct from source account
	newSrcCents := srcAccount.cents - amount
	stmt, err := tx.PrepareContext(ctx, "UPDATE account SET cents = ? WHERE id = ?")
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx, newSrcCents, srcAccount.ID)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	// Add to destination account
	newDstCents := dstAccount.cents + amount
	stmt, err = tx.PrepareContext(ctx, "UPDATE account SET cents = ? WHERE id = ?")
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

func createTransaction(ctx context.Context, tx *sql.Tx, pid string, srcAccount account, dstAccount account, srcWallet wallet, dstWallet wallet, finalDstWallet wallet, amount int64) error {
	ctx, span := trace.SpanFromContext(ctx).TracerProvider().Tracer("db").Start(ctx, "createTransaction")
	defer span.End()

	stmt, err := tx.PrepareContext(ctx, insertTransactionQuery)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx, pid, srcWallet.emailAddress, dstWallet.emailAddress, srcWallet.ID, dstWallet.ID, srcAccount.ID, dstAccount.ID, srcAccount.accountType, dstAccount.accountType, finalDstWallet.ID, amount)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	return nil
}

func createWallet(ctx context.Context, tx *sql.Tx, emailAddress string, walletType pb.WalletType) (int64, error) {
	ctx, span := trace.SpanFromContext(ctx).TracerProvider().Tracer("db").Start(ctx, "createWallet")
	defer span.End()

	stmt, err := tx.PrepareContext(ctx, createWalletQuery)
	if err != nil {
		return -1, status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	// Convert enum to string before insert
	var walletTypeStr string
	switch walletType {
	case pb.WalletType_CUSTOMER:
		walletTypeStr = "CUSTOMER"
	case pb.WalletType_MERCHANT:
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

func createAccount(ctx context.Context, tx *sql.Tx, amount int64, accountType string, walletId int64) error {
	ctx, span := trace.SpanFromContext(ctx).TracerProvider().Tracer("db").Start(ctx, "createAccount")
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

func fetchTransaction(ctx context.Context, tx *sql.Tx, pid string) (transaction, error) {
	ctx, span := trace.SpanFromContext(ctx).TracerProvider().Tracer("db").Start(ctx, "fetchTransaction")
	defer span.End()
	var t transaction

	stmt, err := tx.PrepareContext(ctx, selectTransactionQuery)
	if err != nil {
		return t, status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	err = stmt.QueryRowContext(ctx, pid).Scan(&t.pid, &t.srcEmailAddress, &t.dstEmailAddress, &t.srcAccountWalletID, &t.dstAccountWalletID, &t.srcAccountID, &t.dstAccountID, &t.srcAccountType, &t.dstAccountType, &t.finalDstMerchantWalletID, &t.amount)
	if err != nil {
		if err == sql.ErrNoRows {
			return t, status.Error(codes.InvalidArgument, "transaction not found")
		}
		return t, status.Error(codes.Internal, err.Error())
	}

	return t, nil
}

type wallet struct {
	ID           int32
	emailAddress string
	walletType   string
}

type account struct {
	ID          int32
	cents       int64
	accountType string
	walletID    int32
}

type transaction struct {
	ID                       int32
	pid                      string
	srcEmailAddress          string
	dstEmailAddress          string
	srcAccountWalletID       int32
	dstAccountWalletID       int32
	srcAccountID             int32
	dstAccountID             int32
	srcAccountType           string
	dstAccountType           string
	finalDstMerchantWalletID int32
	amount                   int64
}
