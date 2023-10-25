package eodparser

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
	"io"

	"github.com/jbirddog/marketday"
)

type Parser interface {
	Parse([]time.Time) ([][]*marketday.EODData, error)
}

type HeaderParser func(string) error
type RowParser func(string) (*marketday.EODData, error)

func parseFile(path string, headerParser HeaderParser, rowParser RowParser) ([]*marketday.EODData, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	data := make([]*marketday.EODData, 0, 2048)

	if headerParser != nil && scanner.Scan() {
		err := headerParser(scanner.Text())
		if err != nil {
			return nil, err
		}
	}

	for scanner.Scan() {
		result, err := rowParser(scanner.Text())
		if err != nil {
			return nil, err
		}

		if result != nil {
			data = append(data, result)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return data, nil
}

type EODExchStdCSVParser struct {
	DataDir  string
	Exchange string
}

func (p *EODExchStdCSVParser) Parse(dates []time.Time) ([][]*marketday.EODData, error) {
	eodData := make([][]*marketday.EODData, len(dates))

	for i, date := range dates {
		data, err := p.parseFile(date)
		if err != nil {
			return nil, err
		}

		eodData[i] = data
	}

	return eodData, nil
}

func (p *EODExchStdCSVParser) filePath(date time.Time) string {
	fileName := fmt.Sprintf("%s_%d%02d%02d.csv", p.Exchange, date.Year(), date.Month(), date.Day())
	return path.Join(p.DataDir, fileName)
}

func (p *EODExchStdCSVParser) parseFile(date time.Time) ([]*marketday.EODData, error) {
	path := p.filePath(dir, exchange, date)
	return parseFile(path, nil, p.parseRow)
}

func (p *EODExchStdCSVParser) parseHeader(row string) error {
	if row != "Symbol,Date,Open,High,Low,Close,Volume" {
		return errors.New("Expected header as first line")
	}

	return nil
}

func (p *EODExchStdCSVParser) parseRow(row string) (*marketday.EODData, error) {
	parts := strings.Split(row, ",")

	if len(parts) != 7 {
		return nil, errors.New("Expected record to have 7 fields")
	}

	symbol := parts[0]

	date, err := p.parseDate(parts[1])
	if err != nil {
		return nil, err
	}

	var prices [4]float64
	if err := p.parsePrices(parts[2:6], &prices); err != nil {
		return nil, err
	}

	volume, err := strconv.ParseFloat(parts[6], 64)
	if err != nil {
		return nil, err
	}

	data := &marketday.EODData{
		Symbol: symbol,
		Date:   date,
		Open:   prices[0],
		High:   prices[1],
		Low:    prices[2],
		Close:  prices[3],
		Volume: volume,
	}

	return data, nil
}

func (p *EODExchStdCSVParser) parseDate(field string) (time.Time, error) {
	date, err := time.Parse("02-Jan-2006", field)
	if err == nil {
		date = marketday.Day(date.Year(), date.Month(), date.Day())
	}

	return date, err
}

func (p *EODExchStdCSVParser) parsePrices(fields []string, prices *[4]float64) error {
	if len(fields) != 4 {
		return errors.New("Expected 4 price fields")
	}

	for i, f := range fields {
		price, err := strconv.ParseFloat(f, 64)
		if err != nil {
			return err
		}
		prices[i] = price
	}

	return nil
}
