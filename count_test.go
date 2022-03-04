package elasticsearch

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/bxcodec/faker/v3"
	"github.com/stretchr/testify/assert"
)

func TestCount(t *testing.T) {
	es := newElasticsearch()

	targetSize := rand.Intn(20)
	targetKey := faker.UUIDDigit()
	for i := 0; targetSize > i; i++ {
		var data DocBody
		faker.FakeData(&data)
		data.S = targetKey
		es.CreateDocument(&Document{
			Index: indexName,
			ID:    data.Id,
			Body:  data,
		})
	}

	dummySize := rand.Intn(20)
	for i := 0; dummySize > i; i++ {
		var data DocBody
		faker.FakeData(&data)
		data.S = faker.UUIDDigit()
		es.CreateDocument(&Document{
			Index: indexName,
			ID:    data.Id,
			Body:  data,
		})
	}
	es.Refresh()

	t.Run("Exists", func(t *testing.T) {
		status, count, err := es.Count(indexName, fmt.Sprintf(`{
			"query": {
				"term": {
					"s": "%s"
				}
			}
		}`, targetKey))

		assert.NoError(t, err)
		assert.Equal(t, StatusSuccess, status)
		assert.Equal(t, targetSize, count)
	})

	t.Run("Not Exists", func(t *testing.T) {
		status, count, err := es.Count(indexName, fmt.Sprintf(`{
			"query": {
				"term": {
					"s": "%s"
				}
			}
		}`, faker.UUIDDigit()))

		assert.NoError(t, err)
		assert.Equal(t, StatusSuccess, status)
		assert.Equal(t, 0, count)
	})
}
