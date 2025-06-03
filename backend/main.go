package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/sirupsen/logrus"
)

var (
	logger        = logrus.New()
	tunnelManager *SSHTunnelManager
)

// SSH tunnel manager that maintains persistent connections
type SSHTunnelManager struct {
	activeConnections map[string]*SSHConnection
	mutex             sync.Mutex
	controlDir        string
}

// SSH connection information
type SSHConnection struct {
	Username    string
	Hostname    string
	ControlPath string
	Cmd         *exec.Cmd
	LastUsed    time.Time
	Active      bool
}

type SSHConnectionRequest struct {
	Hostname string `json:"hostname"`
	Username string `json:"username"`
}

type DockerContainer struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	Image          string `json:"image"`
	Status         string `json:"status"`
	Ports          string `json:"ports"`
	Labels         string `json:"labels"`         // New field to store raw label string
	ComposeProject string `json:"composeProject"` // Computed field if the container is part of a Compose project
}

// A group of containers under the same Compose project
type ComposeGroup struct {
	Name       string            `json:"name"`
	Status     string            `json:"status"` // e.g. "Running(3)", "Partial(2/3)", etc.
	Containers []DockerContainer `json:"containers"`
}

// Final response structure
type DockerContainerResponse struct {
	ComposeGroups []ComposeGroup    `json:"composeGroups"`
	Ungrouped     []DockerContainer `json:"ungrouped"`
}

// Settings data file path
const settingsFilePath = "/root/docker-extension/settings.json"

func main() {
	var socketPath string
	flag.StringVar(&socketPath, "socket", "/run/guest-services/backend.sock", "Unix domain socket to listen on")
	flag.Parse()

	_ = os.RemoveAll(socketPath)

	logger.SetOutput(os.Stdout)

	// Initialize SSH tunnel manager
	var err error
	tunnelManager, err = NewSSHTunnelManager()
	if err != nil {
		logger.Fatalf("Failed to initialize SSH tunnel manager: %v", err)
	}

	// Start cleanup routine for idle connections (check every minute, timeout after 30 minutes)
	tunnelManager.StartCleanupRoutine(1*time.Minute, 10*time.Minute)

	logMiddleware := middleware.LoggerWithConfig(middleware.LoggerConfig{
		Skipper: middleware.DefaultSkipper,
		Format: `{"time":"${time_rfc3339_nano}","id":"${id}",` +
			`"method":"${method}","uri":"${uri}",` +
			`"status":${status},"error":"${error}"` +
			`}` + "\n",
		CustomTimeFormat: "2006-01-02 15:04:05.00000",
		Output:           logger.Writer(),
	})

	logger.Infof("Starting listening on %s\n", socketPath)
	router := echo.New()
	router.HideBanner = true
	router.Use(logMiddleware)
	startURL := ""

	ln, err := listen(socketPath)
	if err != nil {
		logger.Fatal(err)
	}
	router.Listener = ln

	router.GET("/hello", hello)
	router.POST("/connect", connectToRemoteDocker)
	// Get settings
	router.GET("/settings", getSettings)
	// Save settings
	router.POST("/settings", saveSettings)

	router.POST("/tunnel/open", openTunnel)
	router.POST("/tunnel/close", closeTunnel)
	router.GET("/tunnel/status", getTunnelStatus)
	router.GET("/tunnel/list", listTunnels)

	// Container management endpoints
	router.POST("/container/start", startContainer)
	router.POST("/container/stop", stopContainer)

	// Image management endpoints
	router.POST("/images/list", listImages)

	// Volume management endpoints
	router.POST("/volumes/list", listVolumes)
	router.POST("/volumes/remove", removeVolume)

	// Network management endpoints
	router.POST("/networks/list", listNetworks)
	router.POST("/networks/remove", removeNetwork)

	router.POST("/container/logs", getContainerLogs)
	router.POST("/compose/logs", getComposeLogs)

	router.POST("/dashboard/overview", getDashboardOverview)
	router.POST("/dashboard/resources", getDashboardResources)
	router.POST("/dashboard/systeminfo", getDashboardSystemInfo)
	router.POST("/dashboard/events", getDashboardEvents)

	// Graceful shutdown handling
	c := make(chan os.Signal, 1)
	go func() {
		<-c
		logger.Info("Shutting down, closing all SSH connections...")
		tunnelManager.CloseAllConnections()
		os.Exit(0)
	}()

	logger.Fatal(router.Start(startURL))
}

// Dashboard overview response
type DashboardOverview struct {
	Containers struct {
		Total   int `json:"total"`
		Running int `json:"running"`
		Stopped int `json:"stopped"`
	} `json:"containers"`
	Images struct {
		Total int    `json:"total"`
		Size  string `json:"size"` // Human readable total size
	} `json:"images"`
	Volumes struct {
		Total int    `json:"total"`
		Size  string `json:"size"` // Human readable total size
	} `json:"volumes"`
	Networks struct {
		Total int `json:"total"`
	} `json:"networks"`
	ComposeProjects struct {
		Total   int `json:"total"`
		Running int `json:"running"`
		Partial int `json:"partial"`
		Stopped int `json:"stopped"`
	} `json:"composeProjects"`
}

// Resource usage for a container
type ContainerResource struct {
	ID       string  `json:"id"`
	Name     string  `json:"name"`
	CPUPerc  string  `json:"cpuPerc"`  // e.g., "0.05%"
	CPUUsage float64 `json:"cpuUsage"` // numeric value for charting
	MemUsage string  `json:"memUsage"` // e.g., "1.2GiB / 15.5GiB"
	MemPerc  string  `json:"memPerc"`  // e.g., "7.74%"
	MemValue float64 `json:"memValue"` // numeric value for charting
	NetIO    string  `json:"netIO"`    // e.g., "1.45GB / 2.3GB"
	BlockIO  string  `json:"blockIO"`  // e.g., "423MB / 8.5MB"
}

// Resource usage response
type ResourcesResponse struct {
	Containers []ContainerResource `json:"containers"`
	System     struct {
		CPUUsage    float64 `json:"cpuUsage"`    // percentage
		MemoryUsage float64 `json:"memoryUsage"` // percentage
		DiskUsage   float64 `json:"diskUsage"`   // percentage
	} `json:"system"`
}

// Docker system information
type SystemInfoResponse struct {
	DockerVersion    string `json:"dockerVersion"`
	APIVersion       string `json:"apiVersion"`
	OS               string `json:"os"`
	Architecture     string `json:"architecture"`
	CPUs             int    `json:"cpus"`
	Memory           string `json:"memory"`
	DockerRoot       string `json:"dockerRoot"`
	ServerTime       string `json:"serverTime"`
	ExperimentalMode bool   `json:"experimentalMode"`
}

// Docker event
type DockerEvent struct {
	Time     int64  `json:"time"`
	TimeStr  string `json:"timeStr"`  // Human readable
	Type     string `json:"type"`     // container, image, volume, network
	Action   string `json:"action"`   // create, start, stop, destroy, etc.
	Actor    string `json:"actor"`    // Name/ID of the object
	Status   string `json:"status"`   // success or error (if applicable)
	Message  string `json:"message"`  // Additional details
	Category string `json:"category"` // info, warning, error
}

// Events response
type EventsResponse struct {
	Events []DockerEvent `json:"events"`
}

// Request for dashboard endpoints
type DashboardRequest struct {
	Hostname string `json:"hostname"`
	Username string `json:"username"`
}

// Add these handler functions to your main.go

// Get dashboard overview statistics
func getDashboardOverview(ctx echo.Context) error {
	var req DashboardRequest
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	if req.Hostname == "" || req.Username == "" {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Missing required fields"})
	}

	// Gather container statistics - using simpler commands
	containerCmd := "docker ps -a | wc -l && docker ps | wc -l"
	containerOutput, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, containerCmd)
	if err != nil {
		logger.Errorf("Error getting container stats: %v", err)
		return ctx.JSON(http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("Failed to get container statistics: %v", err),
		})
	}

	// Parse container counts (accounting for header row)
	containerLines := strings.Split(strings.TrimSpace(string(containerOutput)), "\n")
	totalContainers, runningContainers := 0, 0
	if len(containerLines) >= 2 {
		total, err := strconv.Atoi(strings.TrimSpace(containerLines[0]))
		if err == nil {
			// Subtract 1 for the header row
			totalContainers = total - 1
		}

		running, err := strconv.Atoi(strings.TrimSpace(containerLines[1]))
		if err == nil {
			// Subtract 1 for the header row
			runningContainers = running - 1
		}
	}

	// Ensure we don't have negative values due to header subtraction
	if totalContainers < 0 {
		totalContainers = 0
	}
	if runningContainers < 0 {
		runningContainers = 0
	}

	// Gather image statistics - simpler approach
	imageCmd := "docker images | wc -l"
	imageOutput, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, imageCmd)
	if err != nil {
		logger.Errorf("Error getting image stats: %v", err)
		return ctx.JSON(http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("Failed to get image statistics: %v", err),
		})
	}

	// Parse image count (accounting for header row)
	totalImages := 0
	if len(imageOutput) > 0 {
		count, err := strconv.Atoi(strings.TrimSpace(string(imageOutput)))
		if err == nil && count > 0 {
			totalImages = count - 1 // Subtract 1 for the header
		}
	}

	// Gather disk usage for images (more basic approach)
	imageSizeCmd := "docker system df | grep Images || echo 'N/A'"
	imageSizeOutput, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, imageSizeCmd)
	imageSize := "N/A"
	if err == nil && len(imageSizeOutput) > 0 {
		imageSizeLine := strings.TrimSpace(string(imageSizeOutput))
		if imageSizeLine != "N/A" {
			fields := strings.Fields(imageSizeLine)
			if len(fields) >= 4 {
				imageSize = fields[3]
			}
		}
	}

	// Gather volume statistics
	volumeCmd := "docker volume ls | wc -l"
	volumeOutput, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, volumeCmd)
	totalVolumes := 0
	if err == nil && len(volumeOutput) > 0 {
		count, err := strconv.Atoi(strings.TrimSpace(string(volumeOutput)))
		if err == nil && count > 0 {
			totalVolumes = count - 1 // Subtract 1 for the header
		}
	}

	// Gather network statistics
	networkCmd := "docker network ls | wc -l"
	networkOutput, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, networkCmd)
	totalNetworks := 0
	if err == nil && len(networkOutput) > 0 {
		count, err := strconv.Atoi(strings.TrimSpace(string(networkOutput)))
		if err == nil && count > 0 {
			totalNetworks = count - 1 // Subtract 1 for the header
		}
	}

	// Gather compose project statistics (more tolerant approach)
	composeCmd := "docker ps --format '{{.Labels}}' | grep -c 'com.docker.compose.project' || echo 0"
	composeOutput, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, composeCmd)
	totalCompose := 0
	if err == nil && len(composeOutput) > 0 {
		totalCompose, _ = strconv.Atoi(strings.TrimSpace(string(composeOutput)))
	}

	// Build the response
	overview := DashboardOverview{}
	overview.Containers.Total = totalContainers
	overview.Containers.Running = runningContainers
	overview.Containers.Stopped = totalContainers - runningContainers
	overview.Images.Total = totalImages
	overview.Images.Size = imageSize
	overview.Volumes.Total = totalVolumes
	overview.Volumes.Size = "N/A" // Would need additional commands to calculate
	overview.Networks.Total = totalNetworks
	overview.ComposeProjects.Total = totalCompose
	overview.ComposeProjects.Running = 0 // Would need additional logic to determine
	overview.ComposeProjects.Partial = 0 // Would need additional logic to determine
	overview.ComposeProjects.Stopped = 0 // Would need additional logic to determine

	return ctx.JSON(http.StatusOK, overview)
}

// Get resource usage for containers and system
func getDashboardResources(ctx echo.Context) error {
	var req DashboardRequest
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	if req.Hostname == "" || req.Username == "" {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Missing required fields"})
	}

	// Get container resource usage with docker stats
	// Using a simpler format string that's more likely to work across different Docker versions
	statsCmd := "docker stats --no-stream --format 'table {{.ID}}|{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.MemPerc}}|{{.NetIO}}|{{.BlockIO}}' || docker stats --no-stream"
	statsOutput, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, statsCmd)
	if err != nil {
		logger.Errorf("Error getting resource stats: %v", err)
		return ctx.JSON(http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("Failed to get resource statistics: %v", err),
		})
	}

	// Parse stats output
	lines := strings.Split(strings.TrimSpace(string(statsOutput)), "\n")
	containers := make([]ContainerResource, 0)

	// Skip the header row, process all rows even if we don't have delimiters
	for i := 1; i < len(lines); i++ {
		line := lines[i]

		// Try to parse with our delimiter first
		fields := strings.Split(line, "|")

		// If our custom format didn't work, we'll have the default docker stats output
		// Try to parse using standard spaces as delimiters
		if len(fields) < 7 {
			// Default docker stats has columns separated by variable whitespace
			// We'll make a best effort to parse it
			fields = strings.Fields(line)
			if len(fields) < 7 {
				continue // Not enough fields, skip this line
			}

			// With default stats, order is different:
			// CONTAINER ID, NAME, CPU %, MEM USAGE / LIMIT, MEM %, NET I/O, BLOCK I/O, PIDS

			id := fields[0]
			name := fields[1]
			cpuPerc := fields[2]
			memUsage := fields[3] + " " + fields[4] + " " + fields[5]
			memPerc := fields[6]
			netIO := "N/A"
			blockIO := "N/A"

			if len(fields) >= 8 {
				netIO = fields[7]
			}
			if len(fields) >= 9 {
				blockIO = fields[8]
			}

			// Parse CPU percentage
			cpuValue := 0.0
			if strings.Contains(cpuPerc, "%") {
				cpuValue, _ = strconv.ParseFloat(strings.TrimSuffix(cpuPerc, "%"), 64)
			}

			// Parse memory percentage
			memValue := 0.0
			if strings.Contains(memPerc, "%") {
				memValue, _ = strconv.ParseFloat(strings.TrimSuffix(memPerc, "%"), 64)
			}

			container := ContainerResource{
				ID:       id,
				Name:     name,
				CPUPerc:  cpuPerc,
				CPUUsage: cpuValue,
				MemUsage: memUsage,
				MemPerc:  memPerc,
				MemValue: memValue,
				NetIO:    netIO,
				BlockIO:  blockIO,
			}

			containers = append(containers, container)
			continue
		}

		// If we have our expected delimiter format
		if len(fields) >= 7 {
			// Parse CPU percentage
			cpuPerc := strings.TrimSpace(fields[2])
			cpuValue, _ := strconv.ParseFloat(strings.TrimSuffix(cpuPerc, "%"), 64)

			// Parse memory percentage
			memPerc := strings.TrimSpace(fields[4])
			memValue, _ := strconv.ParseFloat(strings.TrimSuffix(memPerc, "%"), 64)

			container := ContainerResource{
				ID:       strings.TrimSpace(fields[0]),
				Name:     strings.TrimSpace(fields[1]),
				CPUPerc:  cpuPerc,
				CPUUsage: cpuValue,
				MemUsage: strings.TrimSpace(fields[3]),
				MemPerc:  memPerc,
				MemValue: memValue,
				NetIO:    strings.TrimSpace(fields[5]),
				BlockIO:  strings.TrimSpace(fields[6]),
			}

			containers = append(containers, container)
		}
	}

	// Get system resource usage using more basic commands that are more likely to be available
	// First, try a simpler CPU usage check
	cpuUsage := 0.0
	cpuCmd := "top -bn1 | grep '%Cpu' | awk '{print 100 - $8}' || echo 0"
	cpuOutput, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, cpuCmd)
	if err == nil && len(cpuOutput) > 0 {
		cpuUsage, _ = strconv.ParseFloat(strings.TrimSpace(string(cpuOutput)), 64)
	}

	// Memory usage
	memUsage := 0.0
	memCmd := "free | grep Mem | awk '{print $3/$2 * 100}' || echo 0"
	memOutput, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, memCmd)
	if err == nil && len(memOutput) > 0 {
		memUsage, _ = strconv.ParseFloat(strings.TrimSpace(string(memOutput)), 64)
	}

	// Disk usage
	diskUsage := 0.0
	diskCmd := "df -h / | awk 'NR==2 {print $5}' | sed 's/%//' || echo 0"
	diskOutput, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, diskCmd)
	if err == nil && len(diskOutput) > 0 {
		diskUsage, _ = strconv.ParseFloat(strings.TrimSpace(string(diskOutput)), 64)
	}

	// Build the response
	resources := ResourcesResponse{
		Containers: containers,
	}
	resources.System.CPUUsage = cpuUsage
	resources.System.MemoryUsage = memUsage
	resources.System.DiskUsage = diskUsage

	return ctx.JSON(http.StatusOK, resources)
}

// Get Docker system information - simplified to avoid version-specific commands
func getDashboardSystemInfo(ctx echo.Context) error {
	var req DashboardRequest
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	if req.Hostname == "" || req.Username == "" {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Missing required fields"})
	}

	// Create a response with defaults
	info := SystemInfoResponse{
		DockerVersion:    "Unknown",
		APIVersion:       "Unknown",
		OS:               "Unknown",
		Architecture:     "Unknown",
		CPUs:             0,
		Memory:           "Unknown",
		DockerRoot:       "Unknown",
		ServerTime:       "Unknown",
		ExperimentalMode: false,
	}

	// Get Docker version - simple command
	versionCmd := "docker version | grep 'Server Version' | awk '{print $3}' || echo 'Unknown'"
	versionOutput, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, versionCmd)
	if err == nil && len(versionOutput) > 0 {
		info.DockerVersion = strings.TrimSpace(string(versionOutput))
	}

	// Get API version - simple command
	apiCmd := "docker version | grep 'API version' | head -1 | awk '{print $3}' || echo 'Unknown'"
	apiOutput, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, apiCmd)
	if err == nil && len(apiOutput) > 0 {
		info.APIVersion = strings.TrimSpace(string(apiOutput))
	}

	// Get OS info
	osCmd := "uname -s || echo 'Unknown'"
	osOutput, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, osCmd)
	if err == nil && len(osOutput) > 0 {
		info.OS = strings.TrimSpace(string(osOutput))
	}

	// Get architecture
	archCmd := "uname -m || echo 'Unknown'"
	archOutput, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, archCmd)
	if err == nil && len(archOutput) > 0 {
		info.Architecture = strings.TrimSpace(string(archOutput))
	}

	// Get CPU count
	cpuCmd := "nproc || echo 0"
	cpuOutput, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, cpuCmd)
	if err == nil && len(cpuOutput) > 0 {
		cpus, err := strconv.Atoi(strings.TrimSpace(string(cpuOutput)))
		if err == nil {
			info.CPUs = cpus
		}
	}

	// Get memory
	memCmd := "free -h | grep Mem | awk '{print $2}' || echo 'Unknown'"
	memOutput, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, memCmd)
	if err == nil && len(memOutput) > 0 {
		info.Memory = strings.TrimSpace(string(memOutput))
	}

	// Get Docker root directory
	rootCmd := "docker info | grep 'Docker Root Dir' | awk '{print $4}' || echo 'Unknown'"
	rootOutput, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, rootCmd)
	if err == nil && len(rootOutput) > 0 {
		info.DockerRoot = strings.TrimSpace(string(rootOutput))
	}

	// Get server time
	timeCmd := "date +'%Y-%m-%d %H:%M:%S %Z' || echo 'Unknown'"
	timeOutput, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, timeCmd)
	if err == nil && len(timeOutput) > 0 {
		info.ServerTime = strings.TrimSpace(string(timeOutput))
	}

	// Check if experimental mode is enabled
	expCmd := "docker info | grep -q 'Experimental: true' && echo 'true' || echo 'false'"
	expOutput, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, expCmd)
	if err == nil && len(expOutput) > 0 {
		info.ExperimentalMode = strings.TrimSpace(string(expOutput)) == "true"
	}

	return ctx.JSON(http.StatusOK, info)
}

// Get recent Docker events
func getDashboardEvents(ctx echo.Context) error {
	var req DashboardRequest
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	if req.Hostname == "" || req.Username == "" {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Missing required fields"})
	}

	// Get recent Docker events (up to 20 events, simpler command)
	eventsCmd := "docker events --format '{{json .}}' --since 24h --until 0s | tail -20 || echo '[]'"
	eventsOutput, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, eventsCmd)
	if err != nil {
		logger.Errorf("Error getting Docker events: %v", err)
		// Return empty events array rather than an error
		return ctx.JSON(http.StatusOK, EventsResponse{Events: []DockerEvent{}})
	}

	// Parse events
	lines := strings.Split(strings.TrimSpace(string(eventsOutput)), "\n")
	events := make([]DockerEvent, 0)

	for _, line := range lines {
		if line == "" || line == "[]" {
			continue
		}

		// Try to parse the event JSON
		var event struct {
			Time   int64  `json:"time"`
			Status string `json:"status"`
			ID     string `json:"id"`
			From   string `json:"from"`
			Type   string `json:"Type"`
			Actor  struct {
				ID         string            `json:"ID"`
				Attributes map[string]string `json:"Attributes"`
			} `json:"Actor"`
		}

		if err := json.Unmarshal([]byte(line), &event); err != nil {
			logger.Warnf("Failed to parse event: %v", err)
			continue
		}

		// Determine category (info, warning, error)
		category := "info"
		if strings.Contains(event.Status, "kill") || strings.Contains(event.Status, "die") {
			category = "warning"
		} else if strings.Contains(event.Status, "destroy") || strings.Contains(event.Status, "delete") {
			category = "error"
		}

		// Convert time to readable format
		timeStr := time.Unix(event.Time, 0).Format("2006-01-02 15:04:05")

		// Extract name from attributes if available
		name := event.ID
		if event.Actor.Attributes != nil {
			if n, ok := event.Actor.Attributes["name"]; ok {
				name = n
			}
		}

		// Create the event
		dockerEvent := DockerEvent{
			Time:     event.Time,
			TimeStr:  timeStr,
			Type:     event.Type,
			Action:   event.Status,
			Actor:    name,
			Status:   "success", // Assuming success since it was recorded
			Message:  event.From,
			Category: category,
		}

		events = append(events, dockerEvent)
	}

	// Sort events by time (newest first)
	sort.Slice(events, func(i, j int) bool {
		return events[i].Time > events[j].Time
	})

	return ctx.JSON(http.StatusOK, EventsResponse{Events: events})
}

// Request for container logs
type ContainerLogsRequest struct {
	Hostname    string `json:"hostname"`
	Username    string `json:"username"`
	ContainerId string `json:"containerId"`
	Tail        int    `json:"tail"`       // Number of lines to show from the end
	Timestamps  bool   `json:"timestamps"` // Show timestamps
}

// Stream container logs
// Stream container logs with keepalive
func getContainerLogs(ctx echo.Context) error {
	var req ContainerLogsRequest
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	if req.Hostname == "" || req.Username == "" || req.ContainerId == "" {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Missing required fields"})
	}

	// Build docker logs command with appropriate options
	dockerCmd := strings.Builder{}
	dockerCmd.WriteString("docker logs")

	// Add options
	if req.Tail > 0 {
		dockerCmd.WriteString(fmt.Sprintf(" --tail %d", req.Tail))
	}
	if req.Timestamps {
		dockerCmd.WriteString(" --timestamps")
	}

	// Add container ID
	dockerCmd.WriteString(fmt.Sprintf(" %s", req.ContainerId))

	logger.Infof("Executing log command: %s", dockerCmd.String())

	// Execute command using SSH tunnel
	output, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, dockerCmd.String())
	if err != nil {
		logger.Errorf("Error reading logs: %v, output: %s", err, string(output))
		return ctx.JSON(http.StatusInternalServerError, map[string]string{
			"error":  fmt.Sprintf("Failed to read logs: %v", err),
			"output": string(output),
		})
	}

	// Split into lines for returning a JSON array
	lines := strings.Split(string(output), "\n")
	// If the last line is empty, trim it
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}

	return ctx.JSON(http.StatusOK, ContainerLogsResponse{Success: "true", Logs: lines})
}

type ComposeLogsRequest struct {
	Hostname       string `json:"hostname"`
	Username       string `json:"username"`
	ComposeProject string `json:"composeProject"`
	Tail           int    `json:"tail"`       // Number of lines to show from the end
	Timestamps     bool   `json:"timestamps"` // Show timestamps
}

func getComposeLogs(ctx echo.Context) error {
	var req ComposeLogsRequest
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	if req.Hostname == "" || req.Username == "" || req.ComposeProject == "" {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Missing required fields"})
	}

	// Build docker logs command with appropriate options
	dockerCmd := strings.Builder{}
	dockerCmd.WriteString(fmt.Sprintf("docker compose -p %s logs", req.ComposeProject))

	// Add options
	if req.Tail > 0 {
		dockerCmd.WriteString(fmt.Sprintf(" --tail %d", req.Tail))
	}
	if req.Timestamps {
		dockerCmd.WriteString(" --timestamps")
	}

	logger.Infof("Executing log command: %s", dockerCmd.String())

	// Execute command using SSH tunnel
	output, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, dockerCmd.String())
	if err != nil {
		logger.Errorf("Error reading logs: %v, output: %s", err, string(output))
		return ctx.JSON(http.StatusInternalServerError, map[string]string{
			"error":  fmt.Sprintf("Failed to read logs: %v", err),
			"output": string(output),
		})
	}

	// Split into lines for returning a JSON array
	lines := strings.Split(string(output), "\n")
	// If the last line is empty, trim it
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}

	return ctx.JSON(http.StatusOK, ContainerLogsResponse{Success: "true", Logs: lines})
}

// ContainerLogsResponse is what we'll return in JSON.
type ContainerLogsResponse struct {
	Success string   `json:"success"`
	Logs    []string `json:"logs"`
}

// Create a new SSH tunnel manager
func NewSSHTunnelManager() (*SSHTunnelManager, error) {
	// Create directory for SSH control sockets
	controlDir := "/tmp/docker-remote-ssh"
	if err := os.MkdirAll(controlDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create control directory: %v", err)
	}

	return &SSHTunnelManager{
		activeConnections: make(map[string]*SSHConnection),
		controlDir:        controlDir,
	}, nil
}

// Generate connection key for mapping
func connectionKey(username, hostname string) string {
	return fmt.Sprintf("%s@%s", username, hostname)
}

// Create and start a new SSH connection
func (m *SSHTunnelManager) OpenConnection(username, hostname string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	key := connectionKey(username, hostname)

	// Check if connection already exists
	if conn, exists := m.activeConnections[key]; exists && conn.Active {
		// Update last used time
		conn.LastUsed = time.Now()
		logger.Infof("Reusing existing SSH connection for %s", key)
		return nil
	}

	// Create control socket path
	controlPath := filepath.Join(m.controlDir, fmt.Sprintf("ssh-%s.sock", key))

	// Remove existing control socket if it exists
	if _, err := os.Stat(controlPath); err == nil {
		if err := os.Remove(controlPath); err != nil {
			logger.Warnf("Failed to remove existing control socket: %v", err)
		}
	}

	// Start SSH master connection with control socket
	cmd := exec.Command("ssh",
		"-M",              // Master mode for connection sharing
		"-S", controlPath, // Control socket path
		"-o", "ControlPersist=yes",
		"-o", "ServerAliveInterval=10",
		"-o", "ServerAliveCountMax=2",
		"-o", "StrictHostKeyChecking=no",
		"-o", "BatchMode=yes", // Non-interactive mode
		"-N", // Don't execute any command, just forward
		fmt.Sprintf("%s@%s", username, hostname),
	)

	// Start the SSH connection
	logger.Infof("Starting new SSH master connection for %s", key)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start SSH connection: %v", err)
	}

	// Wait a moment for connection to establish
	time.Sleep(1 * time.Second)

	// Check if connection was successful by running a test command
	testCmd := exec.Command("ssh",
		"-o ConnectTimeout=5",
		"-S", controlPath,
		"-o", "StrictHostKeyChecking=no",
		fmt.Sprintf("%s@%s", username, hostname),
		"echo 'Connection test'",
	)

	output, err := testCmd.CombinedOutput()
	if err != nil {
		// Try to kill the master connection if test failed
		cmd.Process.Kill()
		return fmt.Errorf("failed to establish SSH connection: %v, output: %s", err, string(output))
	}

	// Store the connection
	m.activeConnections[key] = &SSHConnection{
		Username:    username,
		Hostname:    hostname,
		ControlPath: controlPath,
		Cmd:         cmd,
		LastUsed:    time.Now(),
		Active:      true,
	}

	logger.Infof("Successfully established SSH connection for %s", key)
	return nil
}

// Close a specific SSH connection
func (m *SSHTunnelManager) CloseConnection(username, hostname string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	key := connectionKey(username, hostname)
	conn, exists := m.activeConnections[key]
	if !exists || !conn.Active {
		return nil // Connection doesn't exist or is already closed
	}

	// Close the connection using control socket
	closeCmd := exec.Command("ssh",
		"-o ConnectTimeout=5",
		"-S", conn.ControlPath,
		"-O", "exit", // Send exit command to master process
		fmt.Sprintf("%s@%s", username, hostname),
	)

	logger.Infof("Closing SSH connection for %s", key)
	output, err := closeCmd.CombinedOutput()
	if err != nil {
		logger.Warnf("Error closing SSH connection cleanly: %v, output: %s", err, string(output))
		// Try to kill the process directly if clean exit fails
		if conn.Cmd != nil && conn.Cmd.Process != nil {
			conn.Cmd.Process.Kill()
		}
	}

	// Clean up the control socket
	if _, err := os.Stat(conn.ControlPath); err == nil {
		if err := os.Remove(conn.ControlPath); err != nil {
			logger.Warnf("Failed to remove control socket: %v", err)
		}
	}

	// Mark as inactive and remove from map
	conn.Active = false
	delete(m.activeConnections, key)

	return nil
}

// Close all active SSH connections
func (m *SSHTunnelManager) CloseAllConnections() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for key, conn := range m.activeConnections {
		if conn.Active {
			// Close the connection using control socket
			closeCmd := exec.Command("ssh",
				"-o ConnectTimeout=5",
				"-S", conn.ControlPath,
				"-O", "exit",
				fmt.Sprintf("%s@%s", conn.Username, conn.Hostname),
			)

			logger.Infof("Closing SSH connection for %s", key)
			output, err := closeCmd.CombinedOutput()
			if err != nil {
				logger.Warnf("Error closing SSH connection cleanly: %v, output: %s", err, string(output))
				// Try to kill the process directly
				if conn.Cmd != nil && conn.Cmd.Process != nil {
					conn.Cmd.Process.Kill()
				}
			}

			// Clean up control socket
			if _, err := os.Stat(conn.ControlPath); err == nil {
				os.Remove(conn.ControlPath)
			}
		}
	}

	// Clear the map
	m.activeConnections = make(map[string]*SSHConnection)
}

// Execute a command using an existing SSH connection
func (m *SSHTunnelManager) ExecuteCommand(username, hostname, command string) ([]byte, error) {
	m.mutex.Lock()
	key := connectionKey(username, hostname)
	conn, exists := m.activeConnections[key]

	if !exists || !conn.Active {
		// No active connection, try to open one
		m.mutex.Unlock()
		if err := m.OpenConnection(username, hostname); err != nil {
			return nil, fmt.Errorf("failed to open connection: %v", err)
		}
		m.mutex.Lock()
		conn = m.activeConnections[key]
	}

	// Update last used time
	conn.LastUsed = time.Now()
	controlPath := conn.ControlPath
	m.mutex.Unlock()

	// Execute command using the control socket
	cmd := exec.Command("ssh",
		"-o ConnectTimeout=5",
		"-S", controlPath,
		"-o", "StrictHostKeyChecking=no",
		fmt.Sprintf("%s@%s", username, hostname),
		command,
	)

	// Run the command and return output
	return cmd.CombinedOutput()
}

// Check if connection is active
func (m *SSHTunnelManager) IsConnectionActive(username, hostname string) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	key := connectionKey(username, hostname)
	conn, exists := m.activeConnections[key]
	if !exists || !conn.Active {
		return false
	}

	// Test connection by running a simple command
	testCmd := exec.Command("ssh",
		"-o ConnectTimeout=5",
		"-S", conn.ControlPath,
		"-o", "StrictHostKeyChecking=no",
		fmt.Sprintf("%s@%s", username, hostname),
		"echo 'Connection test'",
	)

	if err := testCmd.Run(); err != nil {
		logger.Warnf("SSH connection for %s appears to be broken: %v", key, err)
		conn.Active = false
		return false
	}

	return true
}

// Get a list of all active connections
func (m *SSHTunnelManager) GetActiveConnections() []string {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	var connections []string
	for key, conn := range m.activeConnections {
		if conn.Active {
			connections = append(connections, key)
		}
	}
	return connections
}

// Clean up old, unused connections
func (m *SSHTunnelManager) CleanupIdleConnections(idleTimeout time.Duration) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	now := time.Now()
	for key, conn := range m.activeConnections {
		if conn.Active && now.Sub(conn.LastUsed) > idleTimeout {
			logger.Infof("Closing idle SSH connection for %s (idle for %v)", key, now.Sub(conn.LastUsed))

			// Close the connection using control socket
			closeCmd := exec.Command("ssh",
				"-o ConnectTimeout=5",
				"-S", conn.ControlPath,
				"-O", "exit",
				fmt.Sprintf("%s@%s", conn.Username, conn.Hostname),
			)

			output, err := closeCmd.CombinedOutput()
			if err != nil {
				logger.Warnf("Error closing idle SSH connection: %v, output: %s", err, string(output))
				// Try to kill the process directly
				if conn.Cmd != nil && conn.Cmd.Process != nil {
					conn.Cmd.Process.Kill()
				}
			}

			// Clean up control socket
			if _, err := os.Stat(conn.ControlPath); err == nil {
				os.Remove(conn.ControlPath)
			}

			// Mark as inactive and remove from map
			conn.Active = false
			delete(m.activeConnections, key)
		}
	}
}

// Start the background cleanup routine
func (m *SSHTunnelManager) StartCleanupRoutine(checkInterval time.Duration, idleTimeout time.Duration) {
	go func() {
		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()

		for range ticker.C {
			m.CleanupIdleConnections(idleTimeout)
		}
	}()
}

// /////////////////////////////// SSH TunnelAPI Endpoints //////////////////////////////////////
// Request to open/close a tunnel
type TunnelRequest struct {
	Hostname string `json:"hostname"`
	Username string `json:"username"`
}

// Open an SSH tunnel
func openTunnel(ctx echo.Context) error {
	var req TunnelRequest
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	if req.Hostname == "" || req.Username == "" {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Missing required fields"})
	}

	// Open SSH tunnel
	if err := tunnelManager.OpenConnection(req.Username, req.Hostname); err != nil {
		logger.Errorf("Failed to open SSH tunnel: %v", err)
		return ctx.JSON(http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("Failed to open SSH tunnel: %v", err),
		})
	}

	return ctx.JSON(http.StatusOK, map[string]string{
		"success": "true",
		"message": fmt.Sprintf("SSH tunnel opened for %s@%s", req.Username, req.Hostname),
	})
}

// Close an SSH tunnel
func closeTunnel(ctx echo.Context) error {
	var req TunnelRequest
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	if req.Hostname == "" || req.Username == "" {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Missing required fields"})
	}

	// Close SSH tunnel
	if err := tunnelManager.CloseConnection(req.Username, req.Hostname); err != nil {
		logger.Errorf("Failed to close SSH tunnel: %v", err)
		return ctx.JSON(http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("Failed to close SSH tunnel: %v", err),
		})
	}

	return ctx.JSON(http.StatusOK, map[string]string{
		"success": "true",
		"message": fmt.Sprintf("SSH tunnel closed for %s@%s", req.Username, req.Hostname),
	})
}

// Get tunnel status
func getTunnelStatus(ctx echo.Context) error {
	username := ctx.QueryParam("username")
	hostname := ctx.QueryParam("hostname")

	if username == "" || hostname == "" {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Missing username or hostname"})
	}

	isActive := tunnelManager.IsConnectionActive(username, hostname)

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"active":     isActive,
		"connection": fmt.Sprintf("%s@%s", username, hostname),
	})
}

// List all active tunnels
func listTunnels(ctx echo.Context) error {
	activeConnections := tunnelManager.GetActiveConnections()

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"active_tunnels": activeConnections,
	})
}

////////////////////////////////////

// Request for volume operations
type VolumeRequest struct {
	Hostname   string `json:"hostname"`
	Username   string `json:"username"`
	VolumeName string `json:"volumeName"`
}

// Request for network operations
type NetworkRequest struct {
	Hostname  string `json:"hostname"`
	Username  string `json:"username"`
	NetworkId string `json:"networkId"`
}

// List volumes
func listVolumes(ctx echo.Context) error {
	var req struct {
		Hostname string `json:"hostname"`
		Username string `json:"username"`
	}
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	if req.Hostname == "" || req.Username == "" {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Missing required fields"})
	}

	// First, get volume names and driver info
	dockerCommand := "docker volume ls --format '{{.Name}}|{{.Driver}}'"

	// Execute command using SSH tunnel
	output, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, dockerCommand)
	if err != nil {
		logger.Errorf("Error listing volumes: %v, output: %s", err, string(output))
		return ctx.JSON(http.StatusInternalServerError, map[string]string{
			"error":  fmt.Sprintf("Failed to list volumes: %v", err),
			"output": string(output),
		})
	}

	// Parse the output into volume objects
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	volumes := make([]map[string]interface{}, 0, len(lines))

	for _, line := range lines {
		if line == "" {
			continue
		}

		parts := strings.Split(line, "|")
		if len(parts) != 2 {
			logger.Errorf("Invalid format for volume info: %s", line)
			continue
		}

		volumeName := parts[0]
		driver := parts[1]

		// Get detailed info about this volume
		inspectCommand := fmt.Sprintf("docker volume inspect %s", volumeName)
		inspectOutput, inspectErr := tunnelManager.ExecuteCommand(req.Username, req.Hostname, inspectCommand)

		mountpoint := "N/A"
		created := "N/A"
		labels := []string{}

		// If we can get inspect data, extract more info
		if inspectErr == nil && len(inspectOutput) > 0 {
			// Simple parsing approach - in production you'd want to properly parse JSON
			inspectStr := string(inspectOutput)

			// Extract mountpoint
			if mountStart := strings.Index(inspectStr, "\"Mountpoint\": \""); mountStart >= 0 {
				mountStart += 15 // Length of "Mountpoint": "
				if mountEnd := strings.Index(inspectStr[mountStart:], "\""); mountEnd >= 0 {
					mountpoint = inspectStr[mountStart : mountStart+mountEnd]
				}
			}

			// Extract creation time if available
			if createdStart := strings.Index(inspectStr, "\"CreatedAt\": \""); createdStart >= 0 {
				createdStart += 14 // Length of "CreatedAt": "
				if createdEnd := strings.Index(inspectStr[createdStart:], "\""); createdEnd >= 0 {
					created = inspectStr[createdStart : createdStart+createdEnd]
				}
			}

			// Extract labels
			if labelsStart := strings.Index(inspectStr, "\"Labels\": {"); labelsStart >= 0 {
				labelsStart += 11 // Length of "Labels": {
				if labelsEnd := strings.Index(inspectStr[labelsStart:], "}"); labelsEnd >= 0 {
					labelsSection := inspectStr[labelsStart : labelsStart+labelsEnd]
					labelPairs := strings.Split(labelsSection, ",")
					for _, pair := range labelPairs {
						if pair = strings.TrimSpace(pair); pair != "" {
							labels = append(labels, pair)
						}
					}
				}
			}
		}

		volume := map[string]interface{}{
			"name":       volumeName,
			"driver":     driver,
			"mountpoint": mountpoint,
			"created":    created,
			"size":       "N/A", // Size would require more complex commands to determine
			"labels":     labels,
		}
		volumes = append(volumes, volume)
	}

	return ctx.JSON(http.StatusOK, volumes)
}

// Remove a volume
func removeVolume(ctx echo.Context) error {
	var req VolumeRequest
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	if req.Hostname == "" || req.Username == "" || req.VolumeName == "" {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Missing required fields"})
	}

	dockerCommand := fmt.Sprintf("docker volume rm %s", req.VolumeName)

	// Execute command using SSH tunnel
	output, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, dockerCommand)
	if err != nil {
		logger.Errorf("Error removing volume: %v, output: %s", err, string(output))
		return ctx.JSON(http.StatusInternalServerError, map[string]string{
			"error":  fmt.Sprintf("Failed to remove volume: %v", err),
			"output": string(output),
		})
	}

	return ctx.JSON(http.StatusOK, map[string]string{
		"success": "true",
		"message": fmt.Sprintf("Volume %s removed", req.VolumeName),
	})
}

// List networks
func listNetworks(ctx echo.Context) error {
	var req struct {
		Hostname string `json:"hostname"`
		Username string `json:"username"`
	}
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	if req.Hostname == "" || req.Username == "" {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Missing required fields"})
	}

	// Format: ID|Name|Driver|Scope
	dockerCommand := "docker network ls --format '{{.ID}}|{{.Name}}|{{.Driver}}|{{.Scope}}'"

	// Execute command using SSH tunnel
	output, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, dockerCommand)
	if err != nil {
		logger.Errorf("Error listing networks: %v, output: %s", err, string(output))
		return ctx.JSON(http.StatusInternalServerError, map[string]string{
			"error":  fmt.Sprintf("Failed to list networks: %v", err),
			"output": string(output),
		})
	}

	// Parse the output into network objects
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	networks := make([]map[string]interface{}, 0, len(lines))

	for _, line := range lines {
		if line == "" {
			continue
		}

		parts := strings.Split(line, "|")
		if len(parts) != 4 {
			logger.Errorf("Invalid format for network info: %s", line)
			continue
		}

		networkId := parts[0]
		name := parts[1]
		driver := parts[2]
		scope := parts[3]

		// Now get detailed info about this network
		inspectCmd := fmt.Sprintf("docker network inspect %s", networkId)

		// Execute command using SSH tunnel
		inspectOutput, inspectErr := tunnelManager.ExecuteCommand(req.Username, req.Hostname, inspectCmd)

		subnet := ""
		gateway := ""
		ipamDriver := "default"
		internal := false

		// If we can get inspect data, extract more info
		if inspectErr == nil && len(inspectOutput) > 0 {
			// Simple parsing approach - in production you'd want to properly parse JSON
			inspectStr := string(inspectOutput)

			// Extract IPAM driver
			if driverStart := strings.Index(inspectStr, "\"Driver\": \""); driverStart >= 0 {
				driverStart += 11 // Length of "Driver": "
				if driverEnd := strings.Index(inspectStr[driverStart:], "\""); driverEnd >= 0 {
					ipamDriver = inspectStr[driverStart : driverStart+driverEnd]
				}
			}

			// Extract subnet
			if subnetStart := strings.Index(inspectStr, "\"Subnet\": \""); subnetStart >= 0 {
				subnetStart += 11 // Length of "Subnet": "
				if subnetEnd := strings.Index(inspectStr[subnetStart:], "\""); subnetEnd >= 0 {
					subnet = inspectStr[subnetStart : subnetStart+subnetEnd]
				}
			}

			// Extract gateway
			if gatewayStart := strings.Index(inspectStr, "\"Gateway\": \""); gatewayStart >= 0 {
				gatewayStart += 12 // Length of "Gateway": "
				if gatewayEnd := strings.Index(inspectStr[gatewayStart:], "\""); gatewayEnd >= 0 {
					gateway = inspectStr[gatewayStart : gatewayStart+gatewayEnd]
				}
			}

			// Check if internal
			internal = strings.Contains(inspectStr, "\"Internal\": true")
		}

		network := map[string]interface{}{
			"id":         networkId,
			"name":       name,
			"driver":     driver,
			"scope":      scope,
			"ipamDriver": ipamDriver,
			"subnet":     subnet,
			"gateway":    gateway,
			"internal":   internal,
		}
		networks = append(networks, network)
	}

	return ctx.JSON(http.StatusOK, networks)
}

// Remove a network
func removeNetwork(ctx echo.Context) error {
	var req NetworkRequest
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	if req.Hostname == "" || req.Username == "" || req.NetworkId == "" {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Missing required fields"})
	}

	// SSH to remote host and remove network
	dockerCommand := fmt.Sprintf("docker network rm %s", req.NetworkId)

	// Execute command using SSH tunnel
	output, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, dockerCommand)
	if err != nil {
		logger.Errorf("Error removing network: %v, output: %s", err, string(output))
		return ctx.JSON(http.StatusInternalServerError, map[string]string{
			"error":  fmt.Sprintf("Failed to remove network: %v", err),
			"output": string(output),
		})
	}

	return ctx.JSON(http.StatusOK, map[string]string{
		"success": "true",
		"message": fmt.Sprintf("Network %s removed", req.NetworkId),
	})
}

// Request for container operations
type ContainerRequest struct {
	Hostname    string `json:"hostname"`
	Username    string `json:"username"`
	ContainerId string `json:"containerId"`
}

// Start a container
func startContainer(ctx echo.Context) error {
	var req ContainerRequest
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	if req.Hostname == "" || req.Username == "" || req.ContainerId == "" {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Missing required fields"})
	}

	// Format the docker command
	dockerCommand := fmt.Sprintf("docker start %s", req.ContainerId)

	// Execute command using SSH tunnel
	output, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, dockerCommand)
	if err != nil {
		logger.Errorf("Error starting container: %v, output: %s", err, string(output))
		return ctx.JSON(http.StatusInternalServerError, map[string]string{
			"error":  fmt.Sprintf("Failed to start container: %v", err),
			"output": string(output),
		})
	}

	return ctx.JSON(http.StatusOK, map[string]string{
		"success": "true",
		"message": fmt.Sprintf("Container %s started", req.ContainerId),
	})
}

// Stop a container
func stopContainer(ctx echo.Context) error {
	var req ContainerRequest
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	if req.Hostname == "" || req.Username == "" || req.ContainerId == "" {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Missing required fields"})
	}

	// Format the docker command
	dockerCommand := fmt.Sprintf("docker stop %s", req.ContainerId)

	// Execute command using SSH tunnel
	output, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, dockerCommand)
	if err != nil {
		logger.Errorf("Error stopping container: %v, output: %s", err, string(output))
		return ctx.JSON(http.StatusInternalServerError, map[string]string{
			"error":  fmt.Sprintf("Failed to stop container: %v", err),
			"output": string(output),
		})
	}

	return ctx.JSON(http.StatusOK, map[string]string{
		"success": "true",
		"message": fmt.Sprintf("Container %s stopped", req.ContainerId),
	})
}

// List images
func listImages(ctx echo.Context) error {
	var req struct {
		Hostname string `json:"hostname"`
		Username string `json:"username"`
	}
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	if req.Hostname == "" || req.Username == "" {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Missing required fields"})
	}

	// Format the docker command
	dockerCommand := "docker images --format '{{.ID}}|{{.Repository}}|{{.Tag}}|{{.CreatedSince}}|{{.Size}}'"

	// Execute command using SSH tunnel
	output, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, dockerCommand)
	if err != nil {
		logger.Errorf("Error listing images: %v, output: %s", err, string(output))
		return ctx.JSON(http.StatusInternalServerError, map[string]string{
			"error":  fmt.Sprintf("Failed to list images: %v", err),
			"output": string(output),
		})
	}

	// Parse the output into image objects
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	images := make([]map[string]string, 0, len(lines))

	for _, line := range lines {
		if line == "" {
			continue
		}

		parts := strings.Split(line, "|")
		if len(parts) != 5 {
			logger.Errorf("Invalid format for image info: %s", line)
			continue
		}

		image := map[string]string{
			"id":         parts[0],
			"repository": parts[1],
			"tag":        parts[2],
			"created":    parts[3],
			"size":       parts[4],
		}
		images = append(images, image)
	}

	return ctx.JSON(http.StatusOK, images)
}

// Get settings from file
func getSettings(ctx echo.Context) error {
	// Ensure directory exists
	os.MkdirAll(filepath.Dir(settingsFilePath), 0755)

	// Check if settings file exists
	if _, err := os.Stat(settingsFilePath); os.IsNotExist(err) {
		// Return default settings if no settings exist yet
		defaultSettings := map[string]interface{}{
			"environments": []interface{}{},
			"autoConnect":  false,
		}
		jsonData, _ := json.Marshal(defaultSettings)
		return ctx.String(http.StatusOK, string(jsonData))
	}

	// Read settings file
	data, err := ioutil.ReadFile(settingsFilePath)
	if err != nil {
		logger.Errorf("Error reading settings: %v", err)
		return ctx.JSON(http.StatusInternalServerError, map[string]string{
			"error": "Failed to read settings",
		})
	}

	// Return settings data
	return ctx.String(http.StatusOK, string(data))
}

// Save settings to file
func saveSettings(ctx echo.Context) error {
	// Read request body
	body, err := ioutil.ReadAll(ctx.Request().Body)
	if err != nil {
		logger.Errorf("Error reading request body: %v", err)
		return ctx.JSON(http.StatusBadRequest, map[string]string{
			"error": "Failed to read request body",
		})
	}

	// Validate JSON
	var jsonData interface{}
	if err := json.Unmarshal(body, &jsonData); err != nil {
		logger.Errorf("Invalid JSON: %v", err)
		return ctx.JSON(http.StatusBadRequest, map[string]string{
			"error": "Invalid JSON format",
		})
	}

	// Ensure directory exists
	os.MkdirAll(filepath.Dir(settingsFilePath), 0755)

	// Write settings to file
	if err := ioutil.WriteFile(settingsFilePath, body, 0644); err != nil {
		logger.Errorf("Error writing settings: %v", err)
		return ctx.JSON(http.StatusInternalServerError, map[string]string{
			"error": "Failed to save settings",
		})
	}

	return ctx.JSON(http.StatusOK, map[string]string{
		"success": "true",
	})
}

// connectToRemoteDocker: called from the frontend to list containers
func connectToRemoteDocker(ctx echo.Context) error {
	var req SSHConnectionRequest
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	if req.Hostname == "" || req.Username == "" {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "Missing required fields"})
	}

	// Include Labels in docker ps
	dockerCommand := `docker ps --format '{{.ID}}|{{.Names}}|{{.Image}}|{{.Status}}|{{.Ports}}|{{.Labels}}'`

	output, err := tunnelManager.ExecuteCommand(req.Username, req.Hostname, dockerCommand)
	if err != nil {
		logger.Errorf("Error executing SSH command: %v, output: %s", err, string(output))
		return ctx.JSON(http.StatusInternalServerError, map[string]string{
			"error":  fmt.Sprintf("Failed to connect: %v", err),
			"output": string(output),
		})
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	groupsMap := make(map[string][]DockerContainer)
	ungrouped := []DockerContainer{}

	for _, line := range lines {
		if line == "" {
			continue
		}
		parts := strings.Split(line, "|")
		// ID, Name, Image, Status, Ports, Labels
		if len(parts) != 6 {
			logger.Warnf("Invalid container info: %s", line)
			continue
		}

		container := DockerContainer{
			ID:     parts[0],
			Name:   parts[1],
			Image:  parts[2],
			Status: parts[3],
			Ports:  parts[4],
			Labels: parts[5],
		}

		// Check for compose project
		projectName := parseComposeProjectLabel(container.Labels)
		container.ComposeProject = projectName

		if projectName != "" {
			groupsMap[projectName] = append(groupsMap[projectName], container)
		} else {
			ungrouped = append(ungrouped, container)
		}
	}

	// Build final slice of ComposeGroup
	var composeGroups []ComposeGroup
	for projectName, containers := range groupsMap {
		status := computeGroupStatus(containers)
		composeGroups = append(composeGroups, ComposeGroup{
			Name:       projectName,
			Status:     status,
			Containers: containers,
		})
	}

	// Sort by project name
	sort.Slice(composeGroups, func(i, j int) bool {
		return composeGroups[i].Name < composeGroups[j].Name
	})

	response := DockerContainerResponse{
		ComposeGroups: composeGroups,
		Ungrouped:     ungrouped,
	}
	return ctx.JSON(http.StatusOK, response)
}

func computeGroupStatus(containers []DockerContainer) string {
	if len(containers) == 0 {
		return "No containers"
	}

	total := len(containers)
	countUp := 0
	for _, c := range containers {
		if strings.Contains(strings.ToLower(c.Status), "up") {
			countUp++
		}
	}

	switch {
	case countUp == 0:
		// none up
		return fmt.Sprintf("Stopped(%d)", total)
	case countUp == total:
		// all up
		return fmt.Sprintf("Running(%d)", total)
	default:
		// partial
		return fmt.Sprintf("Partial(%d/%d)", countUp, total)
	}
}

// parseComposeProjectLabel checks if the label string contains "com.docker.compose.project=XYZ"
// and returns the project name if found, or empty string if not found.
func parseComposeProjectLabel(labels string) string {
	// Example labels string might look like:
	//   "com.docker.compose.project=helios,com.docker.compose.version=2.15.1"
	// or it might be empty or have other labels
	pairs := strings.Split(labels, ",")
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if strings.HasPrefix(pair, "com.docker.compose.project=") {
			// Extract everything after =
			return strings.TrimPrefix(pair, "com.docker.compose.project=")
		}
	}
	return ""
}

func listen(path string) (net.Listener, error) {
	return net.Listen("unix", path)
}

func hello(ctx echo.Context) error {
	return ctx.JSON(http.StatusOK, HTTPMessageBody{Message: "hello"})
}

type HTTPMessageBody struct {
	Message string
}
