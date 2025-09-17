package mm

import (
	"context"
	"strconv"

	"github.com/IBM/sarama"
	"github.com/andreistefanciprian/gomicropay/money_movement/internal/db"
	"github.com/andreistefanciprian/gomicropay/money_movement/internal/producer"
	pb "github.com/andreistefanciprian/gomicropay/money_movement/proto"
	"github.com/gogo/status"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Implementation struct {
	db       db.MoneyMovementRepository
	producer sarama.SyncProducer
	pb.UnimplementedMoneyMovementServiceServer
	tracer trace.Tracer
	logger *logrus.Logger
}

func NewMoneyMovementImplementation(db db.MoneyMovementRepository, producer sarama.SyncProducer, tracer trace.Tracer, logger *logrus.Logger) *Implementation {
	return &Implementation{
		db:       db,
		producer: producer,
		tracer:   tracer,
		logger:   logger,
	}
}

func (i *Implementation) Authorize(ctx context.Context, authorizePayload *pb.AuthorizePayload) (*pb.AuthorizeResponse, error) {
	// Start a new span for tracing
	ctx, span := i.tracer.Start(ctx, "Authorize")
	defer span.End()

	span.SetAttributes(
		attribute.String("customer_email", authorizePayload.GetCustomerEmailAddress()),
		attribute.String("merchant_email", authorizePayload.GetMerchantEmailAddress()),
		attribute.String("amount", strconv.Itoa(int(authorizePayload.GetCents()))),
	)

	i.logger.Infof("Authorize called with payload: %+v", authorizePayload)

	if authorizePayload.GetCurrency() != "USD" {
		i.logger.Info("Authorize failed: only accepts USD")
		return nil, status.Error(codes.InvalidArgument, "only accepts USD")
	}
	// Begin transaction
	tx, err := i.db.BeginTransaction(ctx)
	if err != nil {
		i.logger.Infof("Authorize failed: could not begin transaction: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	merchantWallet, err := i.db.FetchWallet(ctx, tx, authorizePayload.MerchantEmailAddress)
	if err != nil {
		i.logger.Infof("Authorize failed: could not fetch merchant wallet: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			i.logger.Errorf("Authorize failed: could not rollback after merchant wallet error: %v", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	customerWallet, err := i.db.FetchWallet(ctx, tx, authorizePayload.CustomerEmailAddress)
	if err != nil {
		i.logger.Infof("Authorize failed: could not fetch customer wallet: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			i.logger.Errorf("Authorize failed: could not rollback after customer wallet error: %v", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	srcAccount, err := i.db.FetchAccount(ctx, tx, customerWallet.ID, "DEFAULT")
	if err != nil {
		i.logger.Infof("Authorize failed: could not fetch src account: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			i.logger.Errorf("Authorize failed: could not rollback after src account error: %v", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	dstAccount, err := i.db.FetchAccount(ctx, tx, customerWallet.ID, "PAYMENT")
	if err != nil {
		i.logger.Infof("Authorize failed: could not fetch dst account: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			i.logger.Errorf("Authorize failed: could not rollback after dst account error: %v", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	i.logger.Debugf("Authorize: transferring %d cents from srcAccount %d to dstAccount %d", authorizePayload.Cents, srcAccount.ID, dstAccount.ID)
	err = i.db.Transfer(ctx, tx, srcAccount, dstAccount, authorizePayload.Cents)
	if err != nil {
		i.logger.Infof("Authorize failed: transfer error: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			i.logger.Errorf("Authorize failed: could not rollback after transfer error: %v", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	pid := uuid.New().String()
	i.logger.Debugf("Authorize: creating transaction with pid %s", pid)
	err = i.db.CreateTransaction(ctx, tx, pid, srcAccount, dstAccount, customerWallet, customerWallet, merchantWallet, authorizePayload.Cents)
	if err != nil {
		i.logger.Infof("Authorize failed: createTransaction error: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			i.logger.Errorf("Authorize failed: could not rollback after createTransaction error: %v", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	// Commit tx
	err = i.db.CommitTransaction(tx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	i.logger.Infof("Authorize succeeded: pid=%s", pid)
	return &pb.AuthorizeResponse{Pid: pid}, nil
}

func (i *Implementation) Capture(ctx context.Context, capturePayload *pb.CapturePayload) (*emptypb.Empty, error) {
	ctx, span := i.tracer.Start(ctx, "Capture")
	defer span.End()

	span.SetAttributes(
		attribute.String("pid", capturePayload.GetPid()),
	)
	i.logger.Infof("Capture called with payload: %+v", capturePayload)

	// Begin transaction
	tx, err := i.db.BeginTransaction(ctx)
	if err != nil {
		i.logger.Infof("Capture failed: could not begin transaction: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	authorizeTransaction, err := i.db.FetchTransaction(ctx, tx, capturePayload.Pid)
	if err != nil {
		i.logger.Infof("Capture failed: could not fetch transaction: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			i.logger.Errorf("Capture failed: could not rollback after fetchTransaction error: %v", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	srcAccount, err := i.db.FetchAccount(ctx, tx, authorizeTransaction.DstAccountWalletID, "PAYMENT")
	if err != nil {
		i.logger.Infof("Capture failed: could not fetch src account: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			i.logger.Errorf("Capture failed: could not rollback after src account error: %v", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	dstMerchantAccount, err := i.db.FetchAccount(ctx, tx, authorizeTransaction.FinalDstMerchantWalletID, "INCOMING")
	if err != nil {
		i.logger.Infof("Capture failed: could not fetch dst merchant account: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			i.logger.Errorf("Capture failed: could not rollback after dst merchant account error: %v", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	i.logger.Debugf("Capture: transferring %d cents from srcAccount %d to dstMerchantAccount %d", authorizeTransaction.Amount, srcAccount.ID, dstMerchantAccount.ID)
	err = i.db.Transfer(ctx, tx, srcAccount, dstMerchantAccount, authorizeTransaction.Amount)
	if err != nil {
		i.logger.Infof("Capture failed: transfer error: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			i.logger.Errorf("Capture failed: could not rollback after transfer error: %v", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	customerWallet, err := i.db.FetchWallet(ctx, tx, authorizeTransaction.SrcEmailAddress)
	if err != nil {
		i.logger.Infof("Capture failed: could not fetch customer wallet: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			i.logger.Errorf("Capture failed: could not rollback after customer wallet error: %v", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	merchantWallet, err := i.db.FetchWalletWithWalletID(ctx, tx, authorizeTransaction.FinalDstMerchantWalletID)
	if err != nil {
		i.logger.Infof("Capture failed: could not fetch merchant wallet: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			i.logger.Errorf("Capture failed: could not rollback after merchant wallet error: %v", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	err = i.db.CreateTransaction(ctx, tx, authorizeTransaction.Pid, srcAccount, dstMerchantAccount, customerWallet, merchantWallet, merchantWallet, authorizeTransaction.Amount)
	if err != nil {
		i.logger.Infof("Capture failed: createTransaction error: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			i.logger.Errorf("Capture failed: could not rollback after createTransaction error: %v", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	// Commit tx
	err = i.db.CommitTransaction(tx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	i.logger.Infof("Transaction succeeded: pid=%s", authorizeTransaction.Pid)

	// Send Kafka message
	i.logger.Infof("Sending Kafka messages for transaction: pid=%s", authorizeTransaction.Pid)
	p := producer.NewMessageProducer(i.producer, i.tracer, i.logger)
	p.ProduceMessage(ctx, authorizeTransaction.Pid, authorizeTransaction.SrcEmailAddress, merchantWallet.EmailAddress, authorizeTransaction.Amount)

	return &emptypb.Empty{}, nil
}

func (i *Implementation) CreateAccount(ctx context.Context, createAccountPayload *pb.CreateAccountPayload) (*emptypb.Empty, error) {
	ctx, span := i.tracer.Start(ctx, "CreateAccount")
	defer span.End()

	span.SetAttributes(
		attribute.String("email_address", createAccountPayload.GetEmailAddress()),
		attribute.String("wallet_type", pb.WalletType.String(createAccountPayload.GetWalletType())),
	)
	i.logger.Infof("CreateAccount called with payload: %+v", createAccountPayload)

	// Begin transaction
	tx, err := i.db.BeginTransaction(ctx)
	if err != nil {
		i.logger.Infof("CreateAccount failed: could not begin transaction: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Create wallet
	walletId, err := i.db.CreateWallet(ctx, tx, createAccountPayload.EmailAddress, createAccountPayload.WalletType)
	if err != nil {
		i.logger.Infof("Create Account failed: createWallet error: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			i.logger.Errorf("Create Account failed: could not rollback after createWallet error: %v", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}

	//  Determine account type
	if createAccountPayload.WalletType == pb.WalletType_CUSTOMER {
		//  Create Customer DEFAULT Account
		err = i.db.CreateAccount(ctx, tx, createAccountPayload.InitialBalanceCents, "DEFAULT", int64(walletId))
		if err != nil {
			i.logger.Infof("Create Account failed: createAccount error: %v", err)
			rollbackErr := tx.Rollback()
			if rollbackErr != nil {
				i.logger.Errorf("Create Account failed: could not rollback after createAccount error: %v", rollbackErr)
				return nil, status.Error(codes.Internal, rollbackErr.Error())
			}
			return nil, err
		}
		//  Create Customer PAYMENT Account
		err = i.db.CreateAccount(ctx, tx, 0, "PAYMENT", int64(walletId))
		if err != nil {
			i.logger.Infof("Create Account failed: createAccount error: %v", err)
			rollbackErr := tx.Rollback()
			if rollbackErr != nil {
				i.logger.Errorf("Create Account failed: could not rollback after createAccount error: %v", rollbackErr)
				return nil, status.Error(codes.Internal, rollbackErr.Error())
			}
			return nil, err
		}

	} else {
		//  Create Merchant INCOMING Account
		err = i.db.CreateAccount(ctx, tx, 0, "INCOMING", int64(walletId))
		if err != nil {
			i.logger.Infof("Create Account failed: createAccount error: %v", err)
			rollbackErr := tx.Rollback()
			if rollbackErr != nil {
				i.logger.Errorf("Create Account failed: could not rollback after createAccount error: %v", rollbackErr)
				return nil, status.Error(codes.Internal, rollbackErr.Error())
			}
			return nil, err
		}
	}

	// Commit tx
	err = i.db.CommitTransaction(tx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	i.logger.Infof("CreateAccount succeeded: %s %s", pb.WalletType.String(createAccountPayload.WalletType), createAccountPayload.EmailAddress)
	return &emptypb.Empty{}, nil
}

func (i *Implementation) CheckBalance(ctx context.Context, checkBalancePayload *pb.CheckBalancePayload) (*pb.CheckBalanceResponse, error) {
	ctx, span := i.tracer.Start(ctx, "CheckBalance")
	defer span.End()

	span.SetAttributes(
		attribute.String("email_address", checkBalancePayload.GetEmailAddress()),
	)
	i.logger.Infof("CheckBalance called with payload: %+v", checkBalancePayload)

	// Begin transaction
	tx, err := i.db.BeginTransaction(ctx)
	if err != nil {
		i.logger.Infof("CheckBalance failed: could not begin transaction: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	// get wallet based on email_address
	wallet, err := i.db.FetchWallet(ctx, tx, checkBalancePayload.EmailAddress)
	if err != nil {
		i.logger.Infof("CheckBalance failed: could not fetch wallet: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			i.logger.Errorf("CheckBalance failed: could not rollback after fetchWallet error: %v", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}
	i.logger.Debugf("Fetched wallet: %+v", wallet)

	// determine account type based on wallet_type
	var accountType string
	if wallet.WalletType == "CUSTOMER" {
		accountType = "DEFAULT"
	} else {
		accountType = "INCOMING"
	}

	// get account data based on wallet_id
	account, err := i.db.FetchAccount(ctx, tx, wallet.ID, accountType)
	if err != nil {
		i.logger.Infof("CheckBalance failed: could not fetch account: %v", err)
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			i.logger.Errorf("CheckBalance failed: could not rollback after fetchAccount error: %v", rollbackErr)
			return nil, status.Error(codes.Internal, rollbackErr.Error())
		}
		return nil, err
	}
	i.logger.Debugf("Fetched account: %+v", account)

	// Commit tx
	err = i.db.CommitTransaction(tx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	i.logger.Infof("CheckBalance succeeded: emailAddress=%s, balanceCents=%d", checkBalancePayload.EmailAddress, account.Cents)
	return &pb.CheckBalanceResponse{
		BalanceCents: account.Cents,
	}, nil
}
