package internal

import (
	"fmt"
	"strings"
	"time"
)

func ARRAY_CONCAT(args ...Value) (Value, error) {
	arr := &ArrayValue{}
	for _, arg := range args {
		subarr, err := arg.ToArray()
		if err != nil {
			return nil, err
		}
		arr.values = append(arr.values, subarr.values...)
	}
	return arr, nil
}

func ARRAY_FIRST(v *ArrayValue) (Value, error) {
	return v.values[0], nil
}

func ARRAY_LAST(v *ArrayValue) (Value, error) {
	return v.values[len(v.values)-1], nil
}

func ARRAY_SLICE(v *ArrayValue, startOffset, endOffset int64) (Value, error) {
	arrLen := int64(len(v.values))

	// Convert negative offsets to positive
	start := startOffset
	if start < 0 {
		start = arrLen + start
	}

	end := endOffset
	if end < 0 {
		end = arrLen + end
	}

	// Clamp to array bounds
	if start < 0 {
		start = 0
	}
	if end >= arrLen {
		end = arrLen - 1
	}

	// Handle edge cases
	if start > end || start >= arrLen {
		return &ArrayValue{}, nil
	}

	// Slice the array (end is inclusive, so we add 1)
	result := &ArrayValue{
		values: v.values[start : end+1],
	}
	return result, nil
}

func ARRAY_LENGTH(v *ArrayValue) (Value, error) {
	return IntValue(len(v.values)), nil
}

func ARRAY_TO_STRING(arr *ArrayValue, delim string, nullText ...string) (Value, error) {
	var elems []string
	for _, v := range arr.values {
		if v == nil {
			if len(nullText) == 0 {
				continue
			} else {
				elems = append(elems, nullText[0])
			}
		} else {
			elems = append(elems, v.Format('t'))
		}
	}
	return StringValue(strings.Join(elems, delim)), nil
}

func GENERATE_ARRAY(start, end Value, step ...Value) (Value, error) {
	var stepValue Value
	if len(step) > 0 {
		stepValue = step[0]
	} else {
		stepValue = IntValue(1)
	}
	return generateArray(start, end, stepValue)
}

func GENERATE_DATE_ARRAY(start, end Value, step ...Value) (Value, error) {
	if len(step) > 2 {
		return nil, fmt.Errorf("invalid step value %v", step)
	}
	var (
		stepValue int64 = 1
		interval        = "DAY"
	)
	if len(step) == 2 {
		stepV, err := step[0].ToInt64()
		if err != nil {
			return nil, err
		}
		intervalV, err := step[1].ToString()
		if err != nil {
			return nil, err
		}
		stepValue = stepV
		interval = intervalV
	} else if len(step) == 1 {
		stepV, err := step[0].ToInt64()
		if err != nil {
			return nil, err
		}
		stepValue = stepV
	}
	return generateDateArray(start, end, int(stepValue), interval)
}

func GENERATE_TIMESTAMP_ARRAY(start, end Value, step int64, part string) (Value, error) {
	if start == nil || end == nil || step == 0 {
		return nil, nil
	}
	isLT, err := start.LTE(end)
	if err != nil {
		return nil, err
	}
	arr := &ArrayValue{}
	isPositiveStepValue := step > 0
	if isLT && !isPositiveStepValue {
		// start less than end and step is negative value
		return arr, nil
	} else if !isLT && isPositiveStepValue {
		// start greater than end and step is positive value
		return arr, nil
	}
	cur := start
	for {
		arr.values = append(arr.values, cur)
		after, err := cur.(TimestampValue).AddValueWithPart(time.Duration(step), part)
		if err != nil {
			return nil, err
		}
		if isLT {
			cond, err := after.LTE(end)
			if err != nil {
				return nil, err
			}
			if !cond {
				break
			}
		} else {
			cond, err := after.GTE(end)
			if err != nil {
				return nil, err
			}
			if !cond {
				break
			}
		}
		cur = after
	}
	return arr, nil
}

func generateArray(start, end, step Value) (Value, error) {
	if start == nil || end == nil || step == nil {
		return nil, nil
	}
	isLT, err := start.LTE(end)
	if err != nil {
		return nil, err
	}
	arr := &ArrayValue{}
	isPositiveStepValue, err := step.GT(IntValue(0))
	if err != nil {
		return nil, err
	}
	if isLT && !isPositiveStepValue {
		// start less than end and step is negative value
		return arr, nil
	} else if !isLT && isPositiveStepValue {
		// start greater than end and step is positive value
		return arr, nil
	}
	cur := start
	for {
		arr.values = append(arr.values, cur)
		after, err := cur.Add(step)
		if err != nil {
			return nil, err
		}
		if isLT {
			cond, err := after.LTE(end)
			if err != nil {
				return nil, err
			}
			if !cond {
				break
			}
		} else {
			cond, err := after.GTE(end)
			if err != nil {
				return nil, err
			}
			if !cond {
				break
			}
		}
		cur = after
	}
	return arr, nil
}

func generateDateArray(start, end Value, step int, interval string) (Value, error) {
	if start == nil || end == nil || step == 0 {
		return nil, nil
	}
	isLT, err := start.LTE(end)
	if err != nil {
		return nil, err
	}
	arr := &ArrayValue{}
	isPositiveStepValue := step > 0
	if isLT && !isPositiveStepValue {
		// start less than end and step is negative value
		return arr, nil
	} else if !isLT && isPositiveStepValue {
		// start greater than end and step is positive value
		return arr, nil
	}
	cur := start
	for {
		arr.values = append(arr.values, cur)
		after, err := cur.(DateValue).AddDateWithInterval(step, interval)
		if err != nil {
			return nil, err
		}
		if isLT {
			cond, err := after.LTE(end)
			if err != nil {
				return nil, err
			}
			if !cond {
				break
			}
		} else {
			cond, err := after.GTE(end)
			if err != nil {
				return nil, err
			}
			if !cond {
				break
			}
		}
		cur = after
	}
	return arr, nil
}

func ARRAY_REVERSE(v *ArrayValue) (Value, error) {
	ret := &ArrayValue{}
	for i := len(v.values) - 1; i >= 0; i-- {
		ret.values = append(ret.values, v.values[i])
	}
	return ret, nil
}

// ARRAY_SUM returns the sum of non-NULL elements, or NULL if there are no such elements.
func ARRAY_SUM(arr *ArrayValue) (Value, error) {
	var sum Value
	for _, v := range arr.values {
		if v == nil {
			continue
		}
		if sum == nil {
			sum = v
			continue
		}
		added, err := sum.Add(v)
		if err != nil {
			return nil, err
		}
		sum = added
	}
	return sum, nil
}

// ARRAY_AVG returns the average of non-NULL elements (same averaging rule as aggregate AVG), or NULL if none.
func ARRAY_AVG(arr *ArrayValue) (Value, error) {
	var sum Value
	var n int64
	for _, v := range arr.values {
		if v == nil {
			continue
		}
		n++
		if sum == nil {
			sum = v
			continue
		}
		added, err := sum.Add(v)
		if err != nil {
			return nil, err
		}
		sum = added
	}
	if sum == nil || n == 0 {
		return nil, nil
	}
	base, err := sum.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(base / float64(n)), nil
}

// ARRAY_MIN returns the minimum non-NULL element, or NULL if the array is empty or all NULL.
func ARRAY_MIN(arr *ArrayValue) (Value, error) {
	var min Value
	for _, v := range arr.values {
		if v == nil {
			continue
		}
		if min == nil {
			min = v
			continue
		}
		isLess, err := v.LT(min)
		if err != nil {
			return nil, err
		}
		if isLess {
			min = v
		}
	}
	return min, nil
}

// ARRAY_MAX returns the maximum non-NULL element, or NULL if the array is empty or all NULL.
func ARRAY_MAX(arr *ArrayValue) (Value, error) {
	var max Value
	for _, v := range arr.values {
		if v == nil {
			continue
		}
		if max == nil {
			max = v
			continue
		}
		isGreater, err := v.GT(max)
		if err != nil {
			return nil, err
		}
		if isGreater {
			max = v
		}
	}
	return max, nil
}

// ARRAY_FIRST_N returns the first N elements (or fewer if the array is shorter).
func ARRAY_FIRST_N(arr *ArrayValue, n int64) (Value, error) {
	if n < 0 {
		return nil, fmt.Errorf("ARRAY_FIRST_N: count must be non-negative")
	}
	if n == 0 {
		return &ArrayValue{}, nil
	}
	l := len(arr.values)
	if l == 0 {
		return &ArrayValue{}, nil
	}
	take := int(n)
	if take > l {
		take = l
	}
	out := make([]Value, take)
	copy(out, arr.values[:take])
	return &ArrayValue{values: out}, nil
}

// ARRAY_LAST_N returns the last N elements.
func ARRAY_LAST_N(arr *ArrayValue, n int64) (Value, error) {
	if n < 0 {
		return nil, fmt.Errorf("ARRAY_LAST_N: count must be non-negative")
	}
	if n == 0 {
		return &ArrayValue{}, nil
	}
	l := len(arr.values)
	if l == 0 {
		return &ArrayValue{}, nil
	}
	take := int(n)
	if take > l {
		take = l
	}
	start := l - take
	out := make([]Value, take)
	copy(out, arr.values[start:])
	return &ArrayValue{values: out}, nil
}

// ARRAY_REMOVE_FIRST_N drops the first N elements.
func ARRAY_REMOVE_FIRST_N(arr *ArrayValue, n int64) (Value, error) {
	if n < 0 {
		return nil, fmt.Errorf("ARRAY_REMOVE_FIRST_N: count must be non-negative")
	}
	l := len(arr.values)
	skip := int(n)
	if skip > l {
		skip = l
	}
	return &ArrayValue{values: arr.values[skip:]}, nil
}

// ARRAY_REMOVE_LAST_N drops the last N elements.
func ARRAY_REMOVE_LAST_N(arr *ArrayValue, n int64) (Value, error) {
	if n < 0 {
		return nil, fmt.Errorf("ARRAY_REMOVE_LAST_N: count must be non-negative")
	}
	l := len(arr.values)
	keep := l - int(n)
	if keep < 0 {
		keep = 0
	}
	return &ArrayValue{values: arr.values[:keep]}, nil
}
