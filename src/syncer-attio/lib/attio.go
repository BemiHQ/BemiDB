package attio

import (
	"bytes"
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
		jsonBody, err := json.Marshal(map[string]interface{}{"limit": ATTIO_API_LIMIT, "offset": offset})
		if err != nil {
			return err
		}
		req, err := http.NewRequest("POST", ATTIO_API_URL+"/objects/"+object+"/records/query", bytes.NewBuffer(jsonBody))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+attio.Config.ApiAccessToken)
		req.Header.Set("Content-Type", "application/json")

		common.LogInfo(attio.Config.CommonConfig, "Sending request to Attio:", req.URL.String(), "with body:", string(jsonBody))
		resp, err := attio.HttpClient.Do(req)
		if err != nil {
			return err
		}

		common.LogDebug(attio.Config.CommonConfig, "Received response from Attio:", resp.Status)
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
			for _, rawPerson := range response.Data {
				var person RecordPerson
				err = json.Unmarshal(rawPerson, &person)
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
