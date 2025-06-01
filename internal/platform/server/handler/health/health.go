package health

import (
	"fmt"
	"net/http"
)

func CheckHandler(w http.ResponseWriter, _ *http.Request) {
	fmt.Fprint(w, "Everything OK")
}
