package main

import (
	"github.com/xeipuuv/gojsonschema"
	"log"
)

var (
	schema string = `{
       "$schema": "http://json-schema.org/draft-04/schema#", 
       "items": {
         "type": "object",
         "properties": {
           "offsets": {
            "items": {
              "items": {
                "type": "integer"
              }, 
              "type": "array",
              "maxItems" : 2,
              "minItems" : 2
            }, 
            "type": "array"
          }, 
          "topic": {
            "type": "string",
            "minLength" : 1,
            "maxLength" : 1024
          }
        }, 
        "additionalProperties": false,
        "required" : ["topic", "offsets"]
      }, 
      "type": "array"
    }`
)

func validate(input_json []byte) bool {
	schema_j, err := gojsonschema.NewSchema(gojsonschema.NewBytesLoader([]byte(schema)))
	if err != nil {
		log.Fatalf("Err %v", err)
		return false
	}
	result, err := schema_j.Validate(gojsonschema.NewBytesLoader(input_json))
	if err != nil {
		log.Fatalf("Validation error %v", err)
		return false
	}
	if !result.Valid() {
		for _, desc := range result.Errors() {
			log.Printf("Error: %s\n", desc)
		}
	}
	return result.Valid()
}
