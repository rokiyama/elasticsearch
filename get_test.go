package elasticsearch

import (
	"testing"

	"github.com/bxcodec/faker/v3"
	"github.com/stretchr/testify/assert"
)

func TestGetSource(t *testing.T) {
	es := newElasticsearch()
	id := faker.UUIDDigit()

	var data DocBody
	faker.FakeData(&data)
	if _, err := es.CreateDocument(&Document{
		Index: indexName,
		ID:    id,
		Body:  data,
	}); err != nil {
		t.FailNow()
	}

	es.Refresh()

	t.Run("Found", func(t *testing.T) {
		var res DocBody
		status, err := es.GetSource(indexName, id, &res)
		assert.NoError(t, err)
		assert.Equal(t, 200, status)
		assert.Equal(t, data.Id, res.Id)
		assert.Equal(t, data.I, res.I)
		assert.Equal(t, data.S, res.S)
		assert.Equal(t, data.B, res.B)
	})

	t.Run("Not Found", func(t *testing.T) {
		var res DocBody
		status, err := es.GetSource(indexName, faker.UUIDDigit(), &res)
		assert.NoError(t, err)
		assert.Equal(t, 404, status)
	})
}
