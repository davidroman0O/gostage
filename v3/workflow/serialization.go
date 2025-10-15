package workflow

import "encoding/json"

// ToJSON serializes a Definition into JSON for storage or transport.
func ToJSON(def Definition) ([]byte, error) {
	return json.Marshal(def)
}

// FromJSON deserializes JSON data into a Definition.
func FromJSON(data []byte) (Definition, error) {
	var def Definition
	if err := json.Unmarshal(data, &def); err != nil {
		return Definition{}, err
	}
	return def, nil
}
