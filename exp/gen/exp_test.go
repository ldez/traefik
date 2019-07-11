package gen

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/traefik/paerser/types"
	"gopkg.in/yaml.v3"
)

type Dudu time.Duration

func (d *Dudu) Foo() {
	*d = Dudu(1 * time.Hour)
}

// func (d Dudu) String() string {
// 	return d.String()
// }

func TestDurationUnmarshal(t *testing.T) {
	type foo struct {
		Val *types.Duration
	}

	contentYaml := `val: 10
`

	f := &foo{}
	err := yaml.Unmarshal([]byte(contentYaml), f)
	require.NoError(t, err)

	fmt.Println(f)

	yu := Dudu(1 * time.Second)
	yu.Foo()
	fmt.Println(yu)

	yi := 1 * time.Second
	fmt.Println(yi)

	// 	contentToml := `val = 10
	// `
	//
	// 	g := &foo{}
	// 	err = toml.Unmarshal([]byte(contentToml), g)
	// 	require.NoError(t, err)
	//
	// 	fmt.Println(g)
}

func TestDurationMarshal(t *testing.T) {
	type foo struct {
		Val types.Duration
	}

	f := &foo{
		Val: types.Duration(1 * time.Second),
	}
	contentYaml, err := yaml.Marshal(f)
	require.NoError(t, err)

	fmt.Println(string(contentYaml))

	yu := Dudu(1 * time.Second)
	yu.Foo()
	fmt.Println(yu)

	yi := 1 * time.Second
	fmt.Println(yi)

	// 	contentToml := `val = 10
	// `
	//
	// 	g := &foo{}
	// 	err = toml.Unmarshal([]byte(contentToml), g)
	// 	require.NoError(t, err)
	//
	// 	fmt.Println(g)
}
