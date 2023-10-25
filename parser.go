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

	"github.com/jbirddog/marketday"
)

type Parser interface {
	Parse([]time.Time) ([][]*marketday.EODData, error)
}

func loadFile(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lines := make([]string, 0, 4096)

	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return lines, nil
}

type EODExchStdCSVParser struct {
	DataDir  string
	Exchange string
}

func (p *EODExchStdCSVParser) Parse(dates []time.Time) ([][]*marketday.EODData, error) {
	eodData := make([][]*marketday.EODData, len(dates))

	for i, date := range dates {
		rawData, err := p.loadFile(p.DataDir, p.Exchange, date)
		if err != nil {
			return nil, err
		}

		data, err := p.parseFileContents(rawData)
		if err != nil {
			return nil, err
		}

		eodData[i] = data
	}

	return eodData, nil
}

func (p *EODExchStdCSVParser) filePath(dir string, exchange string, date time.Time) string {
	fileName := fmt.Sprintf("%s_%d%02d%02d.csv", exchange, date.Year(), date.Month(), date.Day())
	return path.Join(dir, fileName)
}

func (p *EODExchStdCSVParser) loadFile(dir string, exchange string, date time.Time) ([]string, error) {
	return loadFile(p.filePath(dir, exchange, date))
}

func (p *EODExchStdCSVParser) parseFileContents(rawData []string) ([]*marketday.EODData, error) {
	if len(rawData) == 0 || rawData[0] != "Symbol,Date,Open,High,Low,Close,Volume" {
		return nil, errors.New("Expected header as first line")
	}

	rawData = rawData[1:]

	if len(rawData) == 0 {
		return nil, errors.New("Expected records to parse")
	}

	data := make([]*marketday.EODData, len(rawData))

	for i, raw := range rawData {
		eodData, err := p.parseRow(raw)

		if err != nil {
			return nil, err
		}

		data[i] = eodData
	}

	return data, nil
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
