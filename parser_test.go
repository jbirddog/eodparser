package eodparser

import (
	"testing"

	"github.com/jbirddog/marketday"
)

func TestEODExchStdCSVFilePath(t *testing.T) {
	parser := &EODExchStdCSVParser{}

	actual := parser.filePath("/data", "NASDAQ", marketday.Day(2023, 7, 4))
	expected := "/data/NASDAQ_20230704.csv"

	if actual != expected {
		t.Fatalf("Expected eod file name %s, got %s", expected, actual)
	}
}

func TestParseMinimalEODExchangeStdCSVFile(t *testing.T) {
	cases := []struct {
		rawData  []string
		expected []*marketday.EODData
	}{
		// single record
		{
			[]string{
				"Symbol,Date,Open,High,Low,Close,Volume",
				"AACG,30-May-2023,1.5,1.5745,1.48,1.4906,16900",
			},
			[]*marketday.EODData{
				&marketday.EODData{
					Symbol: "AACG",
					Date:   marketday.Day(2023, 5, 30),
					Open:   1.5,
					High:   1.5745,
					Low:    1.48,
					Close:  1.4906,
					Volume: 16900,
				},
			},
		},
		// multiple records
		{
			[]string{
				"Symbol,Date,Open,High,Low,Close,Volume",
				"AACG,30-May-2023,1.5,1.5745,1.48,1.4906,16900",
				"AAL,30-May-2023,14.44,14.75,14.42,14.62,20424600",
			},
			[]*marketday.EODData{
				&marketday.EODData{
					Symbol: "AACG",
					Date:   marketday.Day(2023, 5, 30),
					Open:   1.5,
					High:   1.5745,
					Low:    1.48,
					Close:  1.4906,
					Volume: 16900,
				},
				&marketday.EODData{
					Symbol: "AAL",
					Date:   marketday.Day(2023, 5, 30),
					Open:   14.44,
					High:   14.75,
					Low:    14.42,
					Close:  14.62,
					Volume: 20424600,
				},
			},
		},
		// tests price with no decimal place
		{
			[]string{
				"Symbol,Date,Open,High,Low,Close,Volume",
				"VIA,30-Jun-2023,7,7.1846,6.69,6.96,61000",
			},
			[]*marketday.EODData{
				&marketday.EODData{
					Symbol: "VIA",
					Date:   marketday.Day(2023, 6, 30),
					Open:   7.0,
					High:   7.1846,
					Low:    6.69,
					Close:  6.96,
					Volume: 61000,
				},
			},
		},
		// invalid input - no header
		{
			[]string{
				"VIA,30-Jun-2023,7,7.1846,6.69,6.96,61000",
			},
			nil,
		},
		// invalid input - no data
		{
			[]string{
				"Symbol,Date,Open,High,Low,Close,Volume",
			},
			nil,
		},
		// invalid input - bad date
		{
			[]string{
				"Symbol,Date,Open,High,Low,Close,Volume",
				"VIA,bob,7,7.1846,6.69,6.96,61000",
			},
			nil,
		},
		// invalid input - bad open
		{
			[]string{
				"Symbol,Date,Open,High,Low,Close,Volume",
				"VIA,30-Jun-2023,XXX,7.1846,6.69,6.96,61000",
			},
			nil,
		},
		// invalid input - bad high
		{
			[]string{
				"Symbol,Date,Open,High,Low,Close,Volume",
				"VIA,30-Jun-2023,7,XXX,6.69,6.96,61000",
			},
			nil,
		},
		// invalid input - bad low
		{
			[]string{
				"Symbol,Date,Open,High,Low,Close,Volume",
				"VIA,30-Jun-2023,7,5,XXX,6.96,61000",
			},
			nil,
		},
		// invalid input - bad close
		{
			[]string{
				"Symbol,Date,Open,High,Low,Close,Volume",
				"VIA,30-Jun-2023,7,5,3,XXX,61000",
			},
			nil,
		},
		// invalid input - bad volume
		{
			[]string{
				"Symbol,Date,Open,High,Low,Close,Volume",
				"VIA,30-Jun-2023,7,5,3,1000,XXX",
			},
			nil,
		},
	}

	parser := &EODExchStdCSVParser{}

	for i, c := range cases {
		actual, err := parser.parseFileContents(c.rawData)

		if actual == nil {
			if c.expected != nil {
				t.Fatalf("[%d] Got nil response, expected %v", i, c.expected)
			}

			if err == nil {
				t.Fatalf("[%d] Got nil response, expected error", i)
			}

			continue
		}

		if len(actual) != len(c.expected) {
			t.Fatalf("[%d] Expected %v, got %v", i, c.expected, actual)
		}

		for j, a := range actual {
			expected := c.expected[j]

			if !a.Equal(expected) {
				t.Fatalf("[%d:%d] Expected %v, got %v", i, j, expected, a)
			}
		}
	}
}
