package sqlcbulk

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var paramsRegex = regexp.MustCompile(`(\$(\d+))`)
var valuesRegex = regexp.MustCompile(`(?ms)VALUES \((.*)\)$`)

func FindColumns(sql string) []string {
	return paramsRegex.FindAllString(sql, -1)
}

func ReplaceValues(sql, newSQL string) string {
	return valuesRegex.ReplaceAllLiteralString(sql, "VALUES "+newSQL)
}

func BuildPlaceholders(i int, columnCount int) string {
	var placeholders strings.Builder
	for z := 0; z < columnCount; z++ {
		placeholders.WriteString("$" + strconv.Itoa(1+i+z))

		if z != columnCount-1 {
			placeholders.WriteString(",")
		}
	}

	return "(" + placeholders.String() + ")"
}

func Builder[T any](sql string, arg []T, extractor func(row T) []any) (string, []any, error) {
	argCount := len(arg)

	columns := FindColumns(sql)
	columnCount := len(columns)

	i := 0
	values := make([]any, len(arg)*columnCount)

	insert := func(input ...any) {
		for _, v := range input {
			values[i] = v
			i++
		}
	}

	var valSQL strings.Builder

	for z, row := range arg {
		valSQL.WriteString(BuildPlaceholders(i, columnCount))

		if z != argCount-1 {
			valSQL.WriteString(",")
		}

		rowVals := extractor(row)
		if len(rowVals) != columnCount {
			return "", nil, fmt.Errorf(
				"mismatched param and argument count. received %d, expected %d. value: %+v",
				len(rowVals),
				columnCount,
				rowVals,
			) //nolint:goerr113
		}

		insert(extractor(row)...)
	}

	return ReplaceValues(sql, valSQL.String()), values, nil
}
