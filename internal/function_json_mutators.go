package internal

import (
	"encoding/json"
	"fmt"
	"strings"
)

// JSON_REMOVE removes values at JSON paths (paths use $.field chains; root object document).
func JSON_REMOVE(j JsonValue, paths ...string) (Value, error) {
	var root interface{}
	if err := json.Unmarshal([]byte(j), &root); err != nil {
		return nil, fmt.Errorf("JSON_REMOVE: %w", err)
	}
	for _, p := range paths {
		segs, err := jsonPathSegments(p)
		if err != nil {
			return nil, err
		}
		if err := jsonRemoveAt(&root, segs); err != nil {
			return nil, err
		}
	}
	out, err := json.Marshal(root)
	if err != nil {
		return nil, err
	}
	return JsonValue(out), nil
}

// JSON_SET sets values at path/value pairs. createIfMissing controls whether
// missing path segments are created (matches ZetaSQL JSON_SET semantics).
func JSON_SET(j JsonValue, pairs []Value, createIfMissing bool) (Value, error) {
	if len(pairs)%2 != 0 {
		return nil, fmt.Errorf("JSON_SET: expected path/value pairs")
	}
	var root interface{}
	if err := json.Unmarshal([]byte(j), &root); err != nil {
		return nil, fmt.Errorf("JSON_SET: %w", err)
	}
	for i := 0; i < len(pairs); i += 2 {
		pathStr, err := pairs[i].ToString()
		if err != nil {
			return nil, err
		}
		segs, err := jsonPathSegments(pathStr)
		if err != nil {
			return nil, err
		}
		if err := jsonSetAt(&root, segs, pairs[i+1], createIfMissing); err != nil {
			return nil, err
		}
	}
	out, err := json.Marshal(root)
	if err != nil {
		return nil, err
	}
	return JsonValue(out), nil
}

// JSON_STRIP_NULLS removes JSON nulls recursively.
func JSON_STRIP_NULLS(j JsonValue) (Value, error) {
	var root interface{}
	if err := json.Unmarshal([]byte(j), &root); err != nil {
		return nil, fmt.Errorf("JSON_STRIP_NULLS: %w", err)
	}
	stripped := jsonStripNullsWalk(root)
	out, err := json.Marshal(stripped)
	if err != nil {
		return nil, err
	}
	return JsonValue(out), nil
}

func jsonPathSegments(path string) ([]string, error) {
	p := strings.TrimSpace(path)
	if p == "" || p == "$" {
		return nil, nil
	}
	if !strings.HasPrefix(p, "$") {
		return nil, fmt.Errorf("JSON path must start with $")
	}
	rest := strings.TrimPrefix(p, "$")
	if rest == "" {
		return nil, nil
	}
	if strings.HasPrefix(rest, ".") {
		parts := strings.Split(rest[1:], ".")
		out := make([]string, 0, len(parts))
		for _, q := range parts {
			if q != "" {
				out = append(out, q)
			}
		}
		return out, nil
	}
	return nil, fmt.Errorf("unsupported JSON path: %s", path)
}

func jsonRemoveAt(root *interface{}, segs []string) error {
	if len(segs) == 0 {
		return nil
	}
	m, ok := (*root).(map[string]interface{})
	if !ok {
		return fmt.Errorf("JSON_REMOVE: document root must be a JSON object")
	}
	if len(segs) == 1 {
		delete(m, segs[0])
		return nil
	}
	cur := interface{}(m)
	for _, key := range segs[:len(segs)-1] {
		nm, ok := cur.(map[string]interface{})
		if !ok {
			return nil
		}
		child, exists := nm[key]
		if !exists {
			return nil
		}
		cur = child
	}
	lastMap, ok := cur.(map[string]interface{})
	if !ok {
		return nil
	}
	delete(lastMap, segs[len(segs)-1])
	return nil
}

func jsonSetAt(root *interface{}, segs []string, val Value, createIfMissing bool) error {
	if len(segs) == 0 {
		return nil
	}
	jv, err := valueToJSONNative(val)
	if err != nil {
		return err
	}
	m, ok := (*root).(map[string]interface{})
	if !ok {
		return fmt.Errorf("JSON_SET: root must be a JSON object")
	}
	if len(segs) == 1 {
		m[segs[0]] = jv
		return nil
	}
	cur := interface{}(m)
	for _, key := range segs[:len(segs)-1] {
		nm, ok := cur.(map[string]interface{})
		if !ok {
			return fmt.Errorf("JSON_SET: path not traversable")
		}
		child, exists := nm[key]
		if !exists || child == nil {
			if !createIfMissing {
				return nil
			}
			nm[key] = map[string]interface{}{}
			child = nm[key]
		}
		if _, ok := child.(map[string]interface{}); !ok {
			if !createIfMissing {
				return nil
			}
			nm[key] = map[string]interface{}{}
			child = nm[key]
		}
		cur = child
	}
	lastMap := cur.(map[string]interface{})
	lastMap[segs[len(segs)-1]] = jv
	return nil
}

func valueToJSONNative(val Value) (interface{}, error) {
	if val == nil {
		return nil, nil
	}
	j, err := val.ToJSON()
	if err != nil {
		return nil, err
	}
	var out interface{}
	if err := json.Unmarshal([]byte(j), &out); err != nil {
		return nil, err
	}
	return out, nil
}

func jsonStripNullsWalk(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		for k, vv := range t {
			if vv == nil {
				delete(t, k)
				continue
			}
			t[k] = jsonStripNullsWalk(vv)
		}
		return t
	case []interface{}:
		out := make([]interface{}, 0, len(t))
		for _, elem := range t {
			if elem == nil {
				continue
			}
			out = append(out, jsonStripNullsWalk(elem))
		}
		return out
	default:
		return v
	}
}
