package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/olekukonko/tablewriter"
	bolt "go.etcd.io/bbolt"
)

var currentDB *bolt.DB

func main() {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("> ")
		query, _ := reader.ReadString('\n')
		query = strings.TrimSpace(query)

		if strings.ToUpper(query) == "EXIT" {
			if currentDB != nil {
				currentDB.Close()
			}
			break
		}

		processQuery(query)
	}
}

func processQuery(query string) {
	parts := strings.Fields(query)
	if len(parts) == 0 {
		fmt.Println("No command provided")
		return
	}

	command := strings.ToUpper(parts[0])
	switch command {
	case "CREATE":
		if len(parts) > 1 && strings.ToUpper(parts[1]) == "DATABASE" {
			createDatabase(parts[2])
		} else if len(parts) > 1 && strings.ToUpper(parts[1]) == "TABLE" {
			createTable(query)
		} else {
			fmt.Println("Invalid CREATE command")
		}
	case "USE":
		useDatabase(parts[1])
	case "INSERT":
		insertIntoTable(query)
	case "SELECT":
		selectFromTable(query)
	case "UPDATE":
		updateTable(query)
	case "DELETE":
		deleteFromTable(query)
	case "DROP":
		if len(parts) > 1 && strings.ToUpper(parts[1]) == "TABLE" {
			dropTable(parts[2])
		} else {
			fmt.Println("Invalid DROP command")
		}
	default:
		fmt.Println("Unknown command")
	}
}

func createDatabase(name string) {
	if currentDB != nil {
		currentDB.Close()
	}
	var err error
	currentDB, err = bolt.Open("Dbs/"+name+".db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Database", name, "created and selected")
}

func useDatabase(name string) {
	if currentDB != nil {
		currentDB.Close()
	}
	var err error
	currentDB, err = bolt.Open("Dbs/"+name+".db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Using database", name)
}

func createTable(query string) {
	if currentDB == nil {
		fmt.Println("No database selected")
		return
	}

	// Extract table name and columns
	parts := strings.SplitN(query, "(", 2)
	if len(parts) < 2 {
		fmt.Println("Invalid CREATE TABLE syntax")
		return
	}

	tableName := strings.Fields(parts[0])[2]
	columnsPart := strings.TrimSuffix(parts[1], ")")
	columns := strings.Split(columnsPart, ",")

	// Create table (bucket) and store column definitions
	err := currentDB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(tableName))
		if err != nil {
			return err
		}
		for _, col := range columns {
			colName := strings.Fields(strings.TrimSpace(col))[0]
			err := b.Put([]byte("col:"+colName), []byte(col))
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		fmt.Println("Error creating table:", err)
	} else {
		fmt.Println("Table", tableName, "created")
	}
}

func insertIntoTable(query string) {
	if currentDB == nil {
		fmt.Println("No database selected")
		return
	}

	parts := strings.SplitN(query, "(", 2)
	if len(parts) < 2 {
		fmt.Println("Invalid INSERT syntax")
		return
	}

	tableName := strings.Fields(parts[0])[2]
	columnsPart := strings.SplitN(parts[1], ")", 2)
	columns := strings.Split(columnsPart[0], ",")
	valuesPart := strings.SplitN(columnsPart[1], "(", 2)
	values := strings.Split(strings.TrimSuffix(valuesPart[1], ")"), ",")

	err := currentDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil {
			return fmt.Errorf("table %s does not exist", tableName)
		}
		id, _ := b.NextSequence()
		for i, col := range columns {
			key := fmt.Sprintf("%d:%s", id, strings.TrimSpace(col))
			value := strings.TrimSpace(values[i])
			err := b.Put([]byte(key), []byte(value))
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		fmt.Println("Error inserting into table:", err)
	} else {
		fmt.Println("Record inserted into table", tableName)
	}
}

func selectFromTable(query string) {
	if currentDB == nil {
		fmt.Println("No database selected")
		return
	}

	// Parse the SELECT query
	columns, tableName, whereClause := parseSelectQuery(query)

	err := currentDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil {
			return fmt.Errorf("table %s does not exist", tableName)
		}

		// Retrieve all column headers
		var allHeaders []string
		b.ForEach(func(k, v []byte) error {
			if strings.HasPrefix(string(k), "col:") {
				allHeaders = append(allHeaders, strings.TrimPrefix(string(k), "col:"))
			}
			return nil
		})

		// Determine which columns to select
		var headers []string
		selectedColumns := make(map[string]bool)
		if len(columns) == 0 || columns[0] == "*" {
			headers = allHeaders
			for _, col := range allHeaders {
				selectedColumns[col] = true
			}
		} else {
			for _, col := range columns {
				selectedColumns[col] = true
				headers = append(headers, col)
			}
		}

		// Retrieve data rows
		rows := make(map[string]map[string]string)
		b.ForEach(func(k, v []byte) error {
			if !strings.HasPrefix(string(k), "col:") {
				keyParts := strings.Split(string(k), ":")
				if len(keyParts) < 2 {
					return nil
				}
				id := keyParts[0]
				col := keyParts[1]

				if selectedColumns[col] {
					if _, ok := rows[id]; !ok {
						rows[id] = make(map[string]string)
					}
					rows[id][col] = string(v)
				}
			}
			return nil
		})

		// Filter rows based on WHERE clause
		filteredRows := filterRows(rows, whereClause)

		// Prepare table for display
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader(headers)

		for _, row := range filteredRows {
			var rowData []string
			for _, header := range headers {
				rowData = append(rowData, row[header])
			}
			table.Append(rowData)
		}

		table.Render()
		return nil
	})

	if err != nil {
		fmt.Println("Error selecting from table:", err)
	}
}

func parseSelectQuery(query string) ([]string, string, string) {
	parts := strings.SplitN(query, "FROM", 2)
	columnsPart := strings.TrimSpace(parts[0][7:])
	tableAndWhere := strings.TrimSpace(parts[1])

	tableName := tableAndWhere
	whereClause := ""
	if strings.Contains(tableAndWhere, "WHERE") {
		whereParts := strings.SplitN(tableAndWhere, "WHERE", 2)
		tableName = strings.TrimSpace(whereParts[0])
		whereClause = strings.TrimSpace(whereParts[1])
	}

	columns := strings.Split(columnsPart, ",")
	for i, col := range columns {
		columns[i] = strings.TrimSpace(col)
	}

	return columns, tableName, whereClause
}

func filterRows(rows map[string]map[string]string, whereClause string) map[string]map[string]string {
	if whereClause == "" {
		return rows
	}

	filteredRows := make(map[string]map[string]string)
	for id, row := range rows {
		match := true
		for _, condition := range strings.Split(whereClause, "AND") {
			conditionParts := strings.SplitN(strings.TrimSpace(condition), "=", 2)
			if len(conditionParts) != 2 {
				continue
			}
			col := strings.TrimSpace(conditionParts[0])
			val := strings.TrimSpace(conditionParts[1])
			if row[col] != val {
				match = false
				break
			}
		}
		if match {
			filteredRows[id] = row
		}
	}

	return filteredRows
}

func updateTable(query string) {
	if currentDB == nil {
		fmt.Println("No database selected")
		return
	}

	// Parsing query to extract table name, column, value, and condition
	parts := strings.Split(query, "SET")
	updatePart := parts[1]
	tableName := strings.Fields(parts[0])[1]

	updateParts := strings.Fields(updatePart)
	setColumn := updateParts[0]
	setValue := strings.TrimSuffix(updateParts[2], ";")

	whereIndex := strings.Index(query, "WHERE")
	condition := query[whereIndex+6:]

	err := currentDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil {
			return fmt.Errorf("table %s does not exist", tableName)
		}

		b.ForEach(func(k, v []byte) error {
			if !strings.HasPrefix(string(k), "col:") {
				parts := strings.Split(string(k), ":")
				id := parts[0]
				col := parts[1]

				if col == condition {
					key := fmt.Sprintf("%s:%s", id, setColumn)
					err := b.Put([]byte(key), []byte(setValue))
					if err != nil {
						return err
					}
				}
			}
			return nil
		})
		return nil
	})

	if err != nil {
		fmt.Println("Error updating table:", err)
	} else {
		fmt.Println("Table", tableName, "updated")
	}
}

func deleteFromTable(query string) {
	if currentDB == nil {
		fmt.Println("No database selected")
		return
	}

	// Parsing query to extract table name and condition
	parts := strings.Split(query, "FROM")
	tableName := strings.Fields(parts[1])[0]

	whereIndex := strings.Index(query, "WHERE")
	condition := query[whereIndex+6:]

	err := currentDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil {
			return fmt.Errorf("table %s does not exist", tableName)
		}

		// Iterate over all items to find the ones matching the condition
		b.ForEach(func(k, v []byte) error {
			if !strings.HasPrefix(string(k), "col:") {
				parts := strings.Split(string(k), ":")
				col := parts[1]

				if col == condition {
					b.Delete(k)
				}
			}
			return nil
		})
		return nil
	})

	if err != nil {
		fmt.Println("Error deleting from table:", err)
	} else {
		fmt.Println("Records deleted from table", tableName)
	}
}

func dropTable(tableName string) {
	if currentDB == nil {
		fmt.Println("No database selected")
		return
	}

	err := currentDB.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(tableName))
	})

	if err != nil {
		fmt.Println("Error dropping table:", err)
	} else {
		fmt.Println("Table", tableName, "dropped")
	}
}

// func itob(v uint64) []byte {
// 	b := make([]byte, 8)
// 	b[0] = byte(v >> 56)
// 	b[1] = byte(v >> 48)
// 	b[2] = byte(v >> 40)
// 	b[3] = byte(v >> 32)
// 	b[4] = byte(v >> 24)
// 	b[5] = byte(v >> 16)
// 	b[6] = byte(v >> 8)
// 	b[7] = byte(v)
// 	return b
// }
