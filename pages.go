package main

import _ "embed"

//go:embed web/recorder.html
var recorderHTML string

//go:embed web/app.js
var appJS string

//go:embed web/dashboard.html
var dashboardHTML string
