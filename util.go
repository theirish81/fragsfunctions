package fragsfunctions

import (
	"errors"

	"github.com/theirish81/frags"
)

func GetArg[T any](args map[string]any, key string) (*T, error) {
	if v1, ok := args[key]; ok {
		if v2, ok := v1.(T); ok {
			return &v2, nil
		}
		return nil, errors.New(key + " argument is not of the expected type")
	}
	return nil, errors.New(key + " argument is required")
}

type BasicCollection struct {
	name        string
	description string
	functions   frags.ExternalFunctions
}

func NewBasicCollection(name, description string) BasicCollection {
	return BasicCollection{
		name:        name,
		description: description,
		functions:   make(frags.ExternalFunctions),
	}
}

func (bc *BasicCollection) AddFunction(f frags.ExternalFunction) {
	f.Collection = bc.name
	bc.functions[f.Name] = f
}

func (bc *BasicCollection) Name() string {
	return bc.name
}

func (bc *BasicCollection) Description() string {
	return bc.description
}

func (bc *BasicCollection) AsFunctions() frags.ExternalFunctions {
	return bc.functions
}
