// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
    "bufio"
    "bytes"
    "errors"
    "io"
    "log"
    "strconv"
    "strings"
    "time"

    "github.com/influxdata/influxdb1-client/models"
)

var (
    ErrWrongQuote     = errors.New("wrong quote")
    ErrUnmatchedQuote = errors.New("unmatched quote")
    ErrUnclosed       = errors.New("unclosed parenthesis")
    ErrIllegalQL      = errors.New("illegal InfluxQL")
)

func FindEndWithQuote(data []byte, start int, endchar byte) (end int, unquoted []byte, err error) {
    unquoted = append(unquoted, data[start])
    start++
    for end = start; end < len(data); end++ {
        switch data[end] {
        case endchar:
            unquoted = append(unquoted, data[end])
            end++
            return
        case '\\':
            switch {
            case len(data) == end:
                err = ErrUnmatchedQuote
                return
            case data[end+1] == endchar:
                end++
                unquoted = append(unquoted, data[end])
            default:
                err = ErrWrongQuote
                return
            }
        default:
            unquoted = append(unquoted, data[end])
        }
    }
    err = ErrUnmatchedQuote
    return
}

func ScanToken(data []byte, atEOF bool) (advance int, token []byte, err error) {
    if atEOF && len(data) == 0 {
        return 0, nil, nil
    }

    start := 0
    for ; start < len(data) && data[start] == ' '; start++ {
    }
    if start == len(data) {
        return 0, nil, nil
    }

    switch data[start] {
    case '"':
        advance, token, err = FindEndWithQuote(data, start, '"')
        if err != nil {
            log.Printf("scan token error: %s\n", err)
        }
        return
    case '\'':
        advance, token, err = FindEndWithQuote(data, start, '\'')
        if err != nil {
            log.Printf("scan token error: %s\n", err)
        }
        return
    case '(':
        advance = bytes.IndexByte(data[start:], ')')
        if advance == -1 {
            err = ErrUnclosed
        } else {
            advance += start + 1
        }
    case '[':
        advance = bytes.IndexByte(data[start:], ']')
        if advance == -1 {
            err = ErrUnclosed
        } else {
            advance += start + 1
        }
    case '{':
        advance = bytes.IndexByte(data[start:], '}')
        if advance == -1 {
            err = ErrUnclosed
        } else {
            advance += start + 1
        }
    default:
        advance = bytes.IndexFunc(data[start:], func(r rune) bool {
            return r == ' '
        })
        if advance == -1 {
            advance = len(data)
        } else {
            advance += start
        }

    }
    if err != nil {
        log.Printf("scan token error: %s\n", err)
        return
    }

    token = data[start:advance]
    // fmt.Printf("%s (%d, %d) = %s\n", data, start, advance, token)
    return
}

func GetMeasurementFromInfluxQL(q string) (m string, err error) {
    buf := bytes.NewBuffer([]byte(q))
    scanner := bufio.NewScanner(buf)
    scanner.Buffer([]byte(q), len(q))
    scanner.Split(ScanToken)
    var tokens []string
    for scanner.Scan() {
        tokens = append(tokens, scanner.Text())
    }
    // fmt.Printf("%v\n", tokens)

    for i := 0; i < len(tokens); i++ {
        // fmt.Printf("%v\n", tokens[i])
        if strings.ToLower(tokens[i]) == "from" {
            if i+1 < len(tokens) {
                m = getMeasurement(tokens[i+1:])
                return
            }
        }
    }

    return "", ErrIllegalQL
}

func getMeasurement(tokens []string) (m string) {
    if len(tokens) >= 2 && strings.HasPrefix(tokens[1], ".") {
        m = tokens[1]
        m = m[1:]
        if m[0] == '"' || m[0] == '\'' {
            m = m[1 : len(m)-1]
        }
        return
    }

    m = tokens[0]
    if m[0] == '/' {
        return m
    }

    if m[0] == '"' || m[0] == '\'' {
        m = m[1 : len(m)-1]
        return
    }

    index := strings.IndexByte(m, '.')
    if index == -1 {
        return
    }

    m = m[index+1:]
    if m[0] == '"' || m[0] == '\'' {
        m = m[1 : len(m)-1]
    }
    return
}

func ScanKey(pointbuf []byte) (key string, err error) {
    var keybuf [100]byte
    keyslice := keybuf[0:0]
    buflen := len(pointbuf)
    for i := 0; i < buflen; i++ {
        c := pointbuf[i]
        switch c {
        case '\\':
            i++
            keyslice = append(keyslice, pointbuf[i])
        case ' ', ',':
            key = string(keyslice)
            return
        default:
            keyslice = append(keyslice, c)
        }
    }
    return "", io.EOF
}

func ScanSpace(buf []byte) (cnt int) {
    buflen := len(buf)
    for i := 0; i < buflen; {
        switch buf[i] {
        case '\\':
            i += 2
        case '"':
            end, _, err := FindEndWithQuote(buf, i, '"')
            if err != nil {
                log.Printf("scan quote error: %s\n", err)
                return
            }
            i = end
        case ' ', '\t':
            if i == 0 || (buf[i-1] != ' ' && buf[i-1] != '\t') {
                cnt++
            }
            i++
        default:
            i++
        }
    }
    return
}

func ScanTime(buf []byte) (int, bool) {
    i := len(buf) - 1
    for ; i >= 0; i-- {
        if buf[i] < '0' || buf[i] > '9' {
            break
        }
    }
    return i, i > 0 && i < len(buf) - 1 && (buf[i] == ' ' || buf[i] == '\t' || buf[i] == 0)
}

func LineToNano(line []byte, precision string) []byte {
    line = bytes.TrimRight(line, " \t\r\n")
    pos, found := ScanTime(line)
    if found {
        if precision == "ns" {
            return line
        } else if precision == "u" {
            return append(line, []byte("000")...)
        } else if precision == "ms" {
            return append(line, []byte("000000")...)
        } else if precision == "s" {
            return append(line, []byte("000000000")...)
        } else {
            mul := models.GetPrecisionMultiplier(precision)
            nano := BytesToInt64(line[pos+1:]) * mul
            bytenano := Int64ToBytes(nano)
            return bytes.Join([][]byte{line[:pos], bytenano}, []byte(" "))
        }
    } else {
        return append(line, []byte(" " + strconv.FormatInt(time.Now().UnixNano(), 10))...)
    }
}

func Int64ToBytes(n int64) []byte {
    return []byte(strconv.FormatInt(n, 10))
}

func BytesToInt64(buf []byte) int64 {
    var res int64 = 0
    var length = len(buf)
    for i := 0; i < length; i++ {
        res = res * 10 + int64(buf[i]-'0')
    }
    return res
}

// faster than bytes.TrimRight, not sure why.
func TrimRight(p []byte, s []byte) (r []byte) {
    r = p
    if len(r) == 0 {
        return
    }

    i := len(r) - 1
    for ; bytes.IndexByte(s, r[i]) != -1; i-- {
    }
    return r[0 : i+1]
}
