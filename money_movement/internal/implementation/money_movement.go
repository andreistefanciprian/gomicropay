package mm

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/IBM/sarama"
	"github.com/andreistefanciprian/gomicropay/money_movement/internal/producer"
	pb "github.com/andreistefanciprian/gomicropay/money_movement/proto"
	"github.com/gogo/status"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/emptypb"
)

var logLevel = getLogLevel()

func getLogLevel() string {
	lvl := os.Getenv("LOG_LEVEL")
	if lvl == "" {
		return "info"
	}
	return strings.ToLower(lvl)
}

func logInfo(format string, v ...interface{}) {
	if logLevel == "info" || logLevel == "debug" {
		log.Printf("INFO: "+format, v...)
	}
}

func logDebug(format string, v ...interface{}) {
	if logLevel == "debug" {
		log.Printf("DEBUG: "+format, v...)
	}
}

const (
	selectTransactionQuery = "SELECT pid, src_user_id, dst_user_id, src_wallet_id, dst_wallet_id, src_account_id, dst_account_id, src_account_type, dst_account_type, final_dst_merchant_wallet_id, amount FROM transaction WHERE pid = ?"
	insertTransactionQuery = "INSERT INTO transaction (pid, src_user_id, dst_user_id, src_wallet_id, dst_wallet_id, src_account_id, dst_account_id, src_account_type, dst_account_type, final_dst_merchant_wallet_id, amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

type Implementation struct {
	db       *sql.DB
	producer sarama.SyncProducer
	pb.UnimplementedMoneyMovementServiceServer
}

func NewMoneyMovementImplementation(db *sql.DB, producer sarama.SyncProducer) *Implementation {
	return &Implementation{
		db:       db,
		producer: producer,
	}
}

func (i *Implementation) Authorize(ctx context.Context, authorizePayload *pb.AuthorizePayload) (*pb.AuthorizeResponse, error) {
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

	merchantWallet, err := fetchWallet(tx, authorizePayload.MerchantWalletUserId)
	if err != nil {
		logInfo("Authorize failed: could not fetch merchant wallet: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("Authorize failed: could not rollback after merchant wallet error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	customerWallet, err := fetchWallet(tx, authorizePayload.CustomerWalletUserId)
	if err != nil {
		logInfo("Authorize failed: could not fetch customer wallet: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("Authorize failed: could not rollback after customer wallet error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	srcAccount, err := fetchAccount(tx, customerWallet.ID, "DEFAULT")
	if err != nil {
		logInfo("Authorize failed: could not fetch src account: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("Authorize failed: could not rollback after src account error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	dstAccount, err := fetchAccount(tx, customerWallet.ID, "PAYMENT")
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
	err = transfer(tx, srcAccount, dstAccount, authorizePayload.Cents)
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
	err = createTransaction(tx, pid, srcAccount, dstAccount, customerWallet, customerWallet, merchantWallet, authorizePayload.Cents)
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
	logInfo("Capture called with payload: %+v", capturePayload)
	// Begin the transaction
	tx, err := i.db.Begin()
	if err != nil {
		logInfo("Capture failed: could not begin transaction: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	authorizeTransaction, err := fetchTransaction(tx, capturePayload.Pid)
	if err != nil {
		logInfo("Capture failed: could not fetch transaction: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("Capture failed: could not rollback after fetchTransaction error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	srcAccount, err := fetchAccount(tx, authorizeTransaction.dstAccountWalletID, "PAYMENT")
	if err != nil {
		logInfo("Capture failed: could not fetch src account: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("Capture failed: could not rollback after src account error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	dstMerchantAccount, err := fetchAccount(tx, authorizeTransaction.finalDstMerchantWalletID, "INCOMING")
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
	err = transfer(tx, srcAccount, dstMerchantAccount, authorizeTransaction.amount)
	if err != nil {
		logInfo("Capture failed: transfer error: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("Capture failed: could not rollback after transfer error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	customerWallet, err := fetchWallet(tx, authorizeTransaction.srcUserID)
	if err != nil {
		logInfo("Capture failed: could not fetch customer wallet: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("Capture failed: could not rollback after customer wallet error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	merchantWallet, err := fetchWalletWithWalletID(tx, authorizeTransaction.finalDstMerchantWalletID)
	if err != nil {
		logInfo("Capture failed: could not fetch merchant wallet: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			fmt.Printf("Capture failed: could not rollback after merchant wallet error: %v\n", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	err = createTransaction(tx, authorizeTransaction.pid, srcAccount, dstMerchantAccount, customerWallet, merchantWallet, merchantWallet, authorizeTransaction.amount)
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
	producer.SendCaptureMessage(i.producer, authorizeTransaction.pid, authorizeTransaction.srcUserID, authorizeTransaction.amount)

	return &emptypb.Empty{}, nil
}

func (i *Implementation) CheckBalance(ctx context.Context, checkBalancePayload *pb.CheckBalancePayload) (*pb.CheckBalanceResponse, error) {
	logInfo("CheckBalance called with payload: %+v", checkBalancePayload)
	// Begin the transaction
	tx, err := i.db.Begin()
	if err != nil {
		logInfo("CheckBalance failed: could not begin transaction: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	// get wallet based on user_id
	wallet, err := fetchWallet(tx, checkBalancePayload.UserId)
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
	account, err := fetchAccount(tx, wallet.ID, accountType)
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

	logInfo("CheckBalance succeeded: userId=%s, balanceCents=%d", checkBalancePayload.UserId, account.cents)
	return &pb.CheckBalanceResponse{
		BalanceCents: account.cents,
	}, nil
}

func fetchWallet(tx *sql.Tx, userID string) (wallet, error) {
	var w wallet

	stmt, err := tx.Prepare("SELECT id, user_id, wallet_type FROM wallet WHERE user_id = ?")
	if err != nil {
		return w, status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	err = stmt.QueryRow(userID).Scan(&w.ID, &w.userID, &w.walletType)
	if err != nil {
		if err == sql.ErrNoRows {
			return w, status.Error(codes.InvalidArgument, err.Error())
		}
		return w, status.Error(codes.Internal, err.Error())
	}

	return w, nil
}

func fetchWalletWithWalletID(tx *sql.Tx, walletID int32) (wallet, error) {
	var w wallet

	stmt, err := tx.Prepare("SELECT id, user_id, wallet_type FROM wallet WHERE id = ?")
	if err != nil {
		return w, status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	err = stmt.QueryRow(walletID).Scan(&w.ID, &w.userID, &w.walletType)
	if err != nil {
		if err == sql.ErrNoRows {
			return w, status.Error(codes.InvalidArgument, err.Error())
		}
		return w, status.Error(codes.Internal, err.Error())
	}

	return w, nil
}

func fetchAccount(tx *sql.Tx, walletID int32, accountType string) (account, error) {
	var a account

	stmt, err := tx.Prepare("SELECT id, cents, account_type, wallet_id FROM account WHERE wallet_id = ? AND account_type = ?")
	if err != nil {
		return a, status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	err = stmt.QueryRow(walletID, accountType).Scan(&a.ID, &a.cents, &a.accountType, &a.walletID)
	if err != nil {
		if err == sql.ErrNoRows {
			return a, status.Error(codes.InvalidArgument, err.Error())
		}
		return a, status.Error(codes.Internal, err.Error())
	}

	return a, nil
}

func transfer(tx *sql.Tx, srcAccount account, dstAccount account, amount int64) error {
	if srcAccount.cents < amount {
		return status.Error(codes.FailedPrecondition, "insufficient funds")
	}

	// Deduct from source account
	newSrcCents := srcAccount.cents - amount
	stmt, err := tx.Prepare("UPDATE account SET cents = ? WHERE id = ?")
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	_, err = stmt.Exec(newSrcCents, srcAccount.ID)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	// Add to destination account
	newDstCents := dstAccount.cents + amount
	stmt, err = tx.Prepare("UPDATE account SET cents = ? WHERE id = ?")
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	_, err = stmt.Exec(newDstCents, dstAccount.ID)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	return nil
}

func createTransaction(tx *sql.Tx, pid string, srcAccount account, dstAccount account, srcWallet wallet, dstWallet wallet, finalDstWallet wallet, amount int64) error {

	stmt, err := tx.Prepare(insertTransactionQuery)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	_, err = stmt.Exec(pid, srcWallet.userID, dstWallet.userID, srcWallet.ID, dstWallet.ID, srcAccount.ID, dstAccount.ID, srcAccount.accountType, dstAccount.accountType, finalDstWallet.ID, amount)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	return nil
}

func fetchTransaction(tx *sql.Tx, pid string) (transaction, error) {
	var t transaction

	stmt, err := tx.Prepare(selectTransactionQuery)
	if err != nil {
		return t, status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	err = stmt.QueryRow(pid).Scan(&t.pid, &t.srcUserID, &t.dstUserID, &t.srcAccountWalletID, &t.dstAccountWalletID, &t.srcAccountID, &t.dstAccountID, &t.srcAccountType, &t.dstAccountType, &t.finalDstMerchantWalletID, &t.amount)
	if err != nil {
		if err == sql.ErrNoRows {
			return t, status.Error(codes.InvalidArgument, err.Error())
		}
		return t, status.Error(codes.Internal, err.Error())
	}

	return t, nil
}

type wallet struct {
	ID         int32
	userID     string
	walletType string
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
	srcUserID                string
	dstUserID                string
	srcAccountWalletID       int32
	dstAccountWalletID       int32
	srcAccountID             int32
	dstAccountID             int32
	srcAccountType           string
	dstAccountType           string
	finalDstMerchantWalletID int32
	amount                   int64
}
