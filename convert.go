package postgres

import (
	"errors"
	"reflect"
)

// Source: https://stackoverflow.com/a/71272123/4576769
func CheckMapTypes(val any) bool {
	log := CreateLogger("convert.CheckMapTypes")

	mapAny, ok := val.(map[string]interface{})
	if ok {
		log.Debugf("mapAny: %v", mapAny)
		return true
	}
	mapStr, ok := val.(map[string]string)
	if ok {
		log.Debugf("mapStr: %v", mapStr)
		return true
	}

	// Ints
	mapInt, ok := val.(map[string]int)
	if ok {
		log.Debugf("mapInt: %v", mapInt)
		return true
	}
	mapInt32, ok := val.(map[string]int32)
	if ok {
		log.Debugf("mapInt32: %v", mapInt32)
		return true
	}
	mapInt64, ok := val.(map[string]int64)
	if ok {
		log.Debugf("mapInt64: %v", mapInt64)
		return true
	}

	// Floats
	mapFloat32, ok := val.(map[string]float32)
	if ok {
		log.Debugf("mapFloat32: %v", mapFloat32)
		return true
	}
	mapFloat64, ok := val.(map[string]float64)
	if ok {
		log.Debugf("mapFloat64: %v", mapFloat64)
		return true
	}

	return false
}

func StructToMap(val interface{}) (map[string]interface{}, error) {
	log := CreateLogger("utils.StructToMap")

	// Check if val is a map
	mapAny, ok := val.(map[string]interface{})
	if ok {
		return mapAny, nil
	}

	//The name of the tag you will use for fields of struct
	const tagTitle = "json"

	var data map[string]interface{} = make(map[string]interface{})
	varType := reflect.TypeOf(val)
	if varType.Kind() != reflect.Struct {
		// Provided value is not an interface, do what you will with that here
		err := errors.New("not a struct")
		log.WithError(err).Error("Value is not an struct")
		return nil, err
	}

	value := reflect.ValueOf(val)
	for i := 0; i < varType.NumField(); i++ {
		if !value.Field(i).CanInterface() {
			//Skip unexported fields
			continue
		}
		tag, ok := varType.Field(i).Tag.Lookup(tagTitle)
		var fieldName string
		if ok && len(tag) > 0 {
			fieldName = tag
		} else {
			fieldName = varType.Field(i).Name
		}
		if varType.Field(i).Type.Kind() != reflect.Struct {
			data[fieldName] = value.Field(i).Interface()
		} else {
			data[fieldName], _ = StructToMap(value.Field(i).Interface())
		}

	}

	return data, nil
}
