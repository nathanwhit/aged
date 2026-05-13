package flagutil

import "strings"

type OptionalValue struct {
	value string
}

func NewOptionalValue(defaultValue string) *OptionalValue {
	return &OptionalValue{value: defaultValue}
}

func (v *OptionalValue) Set(value string) error {
	v.value = value
	return nil
}

func (v *OptionalValue) String() string {
	if v == nil {
		return ""
	}
	return v.value
}

func (v *OptionalValue) IsBoolFlag() bool {
	return true
}

func NormalizeOptionalValueArgs(args []string, names ...string) []string {
	if len(args) == 0 || len(names) == 0 {
		return args
	}
	optional := map[string]struct{}{}
	for _, name := range names {
		name = strings.TrimLeft(strings.TrimSpace(name), "-")
		if name == "" {
			continue
		}
		optional["-"+name] = struct{}{}
		optional["--"+name] = struct{}{}
	}
	if len(optional) == 0 {
		return args
	}
	normalized := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if _, ok := optional[arg]; ok && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
			normalized = append(normalized, arg+"="+args[i+1])
			i++
			continue
		}
		normalized = append(normalized, arg)
	}
	return normalized
}
