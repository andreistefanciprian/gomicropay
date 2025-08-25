package mm

import (
	"context"
	"database/sql"

	"github.com/andreistefanciprian/gomicropay/money_movement/internal/producer"
	pb "github.com/andreistefanciprian/gomicropay/money_movement/proto"
	"github.com/gogo/status"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	selectTransactionQuery = "SELECT pid, src_user_id, dst_user_id, src_wallet_id, dst_wallet_id, src_account_id, dst_account_id,src_account_type, dst_account_type, finalDstMerchantWalletID, amount FROM transaction WHERE pid = ?"
	insertTransactionQuery = "INSERT INTO transaction (pid, src_user_id, dst_user_id, src_wallet_id, dst_wallet_id, src_account_id, dst_account_id,src_account_type, dst_account_type, finalDstMerchantWalletID, amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

type Implementation struct {
	db *sql.DB
	pb.UnimplementedMoneyMovementServiceServer
}

func NewMoneyMovementImplementation(db *sql.DB) *Implementation {
	return &Implementation{
		db: db,
	}
}

func (i *Implementation) Authorize(ctx context.Context, authorizePayload *pb.AuthorizePayload) (*pb.AuthorizeResponse, error) {

	if authorizePayload.GetCurrency() != "USD" {
		return nil, status.Error(codes.InvalidArgument, "only accepts USD")
	}
	// Begin transaction
	tx, err := i.db.Begin()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	merchantWallet, err := fetchWallet(tx, authorizePayload.MerchantWalletUserId)
	if err != nil {
		err := tx.Rollback()
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		return nil, err
	}

	customerWallet, err := fetchWallet(tx, authorizePayload.CustomerWalletUserId)
	if err != nil {
		err := tx.Rollback()
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		return nil, err
	}

	srcAccount, err := fetchAccount(tx, customerWallet.ID, "DEFAULT")
	if err != nil {
		err := tx.Rollback()
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		return nil, err
	}

	dstAccount, err := fetchAccount(tx, merchantWallet.ID, "PAYMENT")
	if err != nil {
		err := tx.Rollback()
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		return nil, err
	}

	err = transfer(tx, srcAccount, dstAccount, authorizePayload.Cents)
	if err != nil {
		err := tx.Rollback()
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		return nil, err
	}

	pid, err := createTransaction(tx, srcAccount, dstAccount, merchantWallet, authorizePayload.Cents)
	if err != nil {
		err := tx.Rollback()
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		return nil, err
	}
	// End transaction
	err = tx.Commit()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.AuthorizeResponse{Pid: pid}, nil
}

func (i *Implementation) Capture(ctx context.Context, capturePayload *pb.CapturePayload) (*emptypb.Empty, error) {
	// Begin the transaction
	tx, err := i.db.Begin()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	authorizeTransaction, err := fetchTransaction(tx, capturePayload.Pid)
	if err != nil {
		err := tx.Rollback()
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		return nil, err
	}

	srcAccount, err := fetchAccount(tx, authorizeTransaction.dstAccountWalletID, "PAYMENT")
	if err != nil {
		err := tx.Rollback()
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		return nil, err
	}

	dstMerchantAccount, err := fetchAccount(tx, authorizeTransaction.finalDstMerchantWalletID, "INCOMING")
	if err != nil {
		err := tx.Rollback()
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		return nil, err
	}

	err = transfer(tx, srcAccount, dstMerchantAccount, authorizeTransaction.amount)
	if err != nil {
		err := tx.Rollback()
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		return nil, err
	}
	merchantWallet, err := fetchWallet(tx, authorizeTransaction.dstUserID)
	if err != nil {
		err := tx.Rollback()
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		return nil, err
	}

	_, err = createTransaction(tx, srcAccount, dstMerchantAccount, merchantWallet, authorizeTransaction.amount)
	if err != nil {
		err := tx.Rollback()
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		return nil, err
	}

	// Commit tx
	err = tx.Commit()
	if err != nil {
		err := tx.Rollback()
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		return nil, err
	}

	producer.SendCaptureMessage(authorizeTransaction.pid, authorizeTransaction.srcUserID, authorizeTransaction.amount)

	return &emptypb.Empty{}, nil
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

func createTransaction(tx *sql.Tx, srcAccount account, dstAccount account, merchantWallet wallet, amount int64) (string, error) {
	pid := uuid.New().String()

	stmt, err := tx.Prepare(insertTransactionQuery)
	if err != nil {
		return "", status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	_, err = stmt.Exec(pid, srcAccount.walletID, dstAccount.walletID, srcAccount.ID, dstAccount.ID, srcAccount.accountType, dstAccount.accountType, merchantWallet.ID, amount)
	if err != nil {
		return "", status.Error(codes.Internal, err.Error())
	}

	return pid, nil
}

func fetchTransaction(tx *sql.Tx, pid string) (transaction, error) {
	var t transaction

	stmt, err := tx.Prepare(selectTransactionQuery)
	if err != nil {
		return t, status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	err = stmt.QueryRow(pid).Scan(&t.ID, &t.pid, &t.srcUserID, &t.dstUserID, &t.srcAccountID, &t.dstAccountID, &t.srcAccountType, &t.dstAccountType, &t.finalDstMerchantWalletID, &t.amount)
	if err != nil {
		if err == sql.ErrNoRows {
			return t, status.Error(codes.InvalidArgument, err.Error())
		}
		return t, status.Error(codes.Internal, err.Error())
	}

	return t, nil
}
