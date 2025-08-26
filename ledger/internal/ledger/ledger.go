package ledger

import "database/sql"

func Insert(db *sql.DB, orderID, userID string, amount int64, operation, transactionDate string) error {
	query := "INSERT INTO ledger (order_id, user_id, amount, operation, transactionDate) VALUES (?, ?, ?, ?, ?)"

	stmt, err := db.Prepare(query)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(orderID, userID, amount, operation, transactionDate)
	if err != nil {
		return err
	}

	return nil
}
