package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/BemiHQ/BemiDB/src/common"
)

const (
	ATTIO_API_URL   = "https://api.attio.com/v2"
	ATTIO_API_LIMIT = 500

	ATTIO_OBJECT_COMPANIES = "companies"
	ATTIO_OBJECT_DEALS     = "deals"
	ATTIO_OBJECT_PEOPLE    = "people"
)

type Attio struct {
	Config     *Config
	HttpClient *http.Client
	Parser     *Parser
}

func NewAttio(config *Config) *Attio {
	return &Attio{
		Config:     config,
		HttpClient: &http.Client{Timeout: 30 * time.Second},
		Parser:     NewParser(config),
	}
}

type ListRecordsResponse struct {
	Data []json.RawMessage `json:"data"`
}

func (attio *Attio) Load(object string, jsonQueueWriter *common.JsonQueueWriter) error {
	offset := 0
	for {
		req, err := http.NewRequest("POST", ATTIO_API_URL+"/objects/"+object+"/records/query", nil)
		if err != nil {
			return err
		}
		q := req.URL.Query()
		q.Add("limit", common.IntToString(ATTIO_API_LIMIT))
		q.Add("offset", common.IntToString(offset))
		req.URL.RawQuery = q.Encode()

		req.Header.Set("Authorization", "Bearer "+attio.Config.ApiAccessToken)
		req.Header.Set("Content-Type", "application/json")

		resp, err := attio.HttpClient.Do(req)
		if err != nil {
			return err
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return fmt.Errorf("failed to list Attio records for object '%s': status %d, body: %s", object, resp.StatusCode, string(body))
		}

		var response ListRecordsResponse
		err = json.NewDecoder(resp.Body).Decode(&response)
		resp.Body.Close()
		if err != nil {
			return err
		}

		switch object {
		case ATTIO_OBJECT_COMPANIES:
			for _, rawCompany := range response.Data {
				var record RecordCompany
				err = json.Unmarshal(rawCompany, &record)
				if err != nil {
					return err
				}
				parsedMap := record.ToMap(attio.Parser)
				err = jsonQueueWriter.Write(parsedMap)
				if err != nil {
					return err
				}
			}
		case ATTIO_OBJECT_DEALS:
			for _, rawDeal := range response.Data {
				var record RecordDeal
				err = json.Unmarshal(rawDeal, &record)
				if err != nil {
					return err
				}
				parsedMap := record.ToMap(attio.Parser)
				err = jsonQueueWriter.Write(parsedMap)
				if err != nil {
					return err
				}
			}
		case ATTIO_OBJECT_PEOPLE:
			for _, rawDeal := range response.Data {
				var person RecordPerson
				err = json.Unmarshal(rawDeal, &person)
				if err != nil {
					return err
				}
				parsedMap := person.ToMap(attio.Parser)
				err = jsonQueueWriter.Write(parsedMap)
				if err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unknown Attio object type: %s", object)
		}

		if len(response.Data) < ATTIO_API_LIMIT {
			break
		}
		offset += ATTIO_API_LIMIT
	}

	return nil
}
