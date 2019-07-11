package gen

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/traefik/paerser/flag"
	"github.com/traefik/paerser/generator"
	"github.com/traefik/traefik/v2/pkg/config/static"
)

func TestGenCLI(t *testing.T) {
	os.RemoveAll(staticCliRef)

	genCLI(t, staticCliRef, "--")
}

func genCLI(t *testing.T, outputFile string, prefix string) {
	element := &static.Configuration{}

	generator.Generate(element)

	flats, err := flag.Encode(element)
	require.NoError(t, err)

	file, err := os.Create(outputFile)
	require.NoError(t, err)

	defer file.Close()

	for i, flat := range flats {
		// if flat.Hidden {
		// 	continue
		// }

		fmt.Fprintln(file, "`"+prefix+strings.ReplaceAll(flat.Name, "[0]", "[n]")+"`:  ")
		if flat.Default == "" {
			fmt.Fprintln(file, flat.Description)
		} else {
			fmt.Fprintln(file, flat.Description+" (Default: ```"+flat.Default+"```)")
		}

		if i < len(flats)-1 {
			fmt.Fprintln(file)
		}
	}
}
