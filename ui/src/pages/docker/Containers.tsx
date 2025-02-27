import React, { useState, useEffect } from 'react';
import { createDockerDesktopClient } from '@docker/extension-api-client';
import {
  Alert,
  Box,
  CircularProgress,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
  IconButton,
  Tooltip,
  Chip,
  Drawer,
  Stack
} from '@mui/material';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import StopIcon from '@mui/icons-material/Stop';
import VisibilityIcon from '@mui/icons-material/Visibility';
import PortIcon from '@mui/icons-material/Devices';
import { Environment, ExtensionSettings } from '../../App';
import AutoRefreshControls from '../../components/AutoRefreshControls';
import ContainerLogs from './ContainerLogs';
import ConfirmationDialog from '../../components/ConfirmationDialog';

// Extended Container interface with ports
export interface DockerContainer {
  id: string;
  name: string;
  image: string;
  status: string;
  ports: string;
  labels?: string; // raw label string from Docker, optional
  composeProject?: string; // if container belongs to a compose project
}

// A group of containers from the same Compose project
export interface ComposeGroup {
  name: string;
  containers: DockerContainer[];
}

// The full response from the backend
export interface ContainersResponse {
  composeGroups: ComposeGroup[];
  ungrouped: DockerContainer[];
}


// Parsed port binding type
interface PortBinding {
  hostPort: string;
  containerPort: string;
  protocol: string;
}

// Error response interface
interface ErrorResponse {
  error: string;
  output?: string;
}

interface ContainersProps {
  activeEnvironment?: Environment;
  settings: ExtensionSettings;
  isLogsOpen: boolean;
  setIsLogsOpen: (isOpen: boolean) => void;
}

const client = createDockerDesktopClient();

function useDockerDesktopClient() {
  return client;
}

const Containers: React.FC<ContainersProps> = ({
                                                 activeEnvironment,
                                                 settings,
                                                 isLogsOpen,
                                                 setIsLogsOpen
                                               }) => {
  const [composeGroups, setComposeGroups] = useState<ComposeGroup[]>([]);
  const [ungroupedContainers, setUngroupedContainers] = useState<DockerContainer[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState('');
  const ddClient = useDockerDesktopClient();

  // Auto-refresh states
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [refreshInterval, setRefreshInterval] = useState(30); // Default 30 seconds
  const [lastRefreshTime, setLastRefreshTime] = useState<Date | null>(null);
  const [isRefreshing, setIsRefreshing] = useState(false); // For refresh indicator overlay

  // Container logs states
  const [selectedContainer, setSelectedContainer] = useState<DockerContainer | null>(null);

  // Confirmation dialog states
  const [confirmDialogOpen, setConfirmDialogOpen] = useState(false);
  const [confirmAction, setConfirmAction] = useState<{
    type: 'start' | 'stop';
    container: DockerContainer | null;
  }>({ type: 'start', container: null });

  // Load containers when active environment changes
  useEffect(() => {
    if (activeEnvironment) {
      loadContainers();
    } else {
      // Clear containers if no environment is selected
      setComposeGroups([]);
      setUngroupedContainers([]);
    }
  }, [activeEnvironment]);

  // Auto-refresh interval setup
  useEffect(() => {
    let intervalId: NodeJS.Timeout | null = null;

    if (autoRefresh && activeEnvironment) {
      intervalId = setInterval(() => {
        loadContainers();
      }, refreshInterval * 1000);
    }

    // Cleanup function
    return () => {
      if (intervalId) {
        clearInterval(intervalId);
      }
    };
  }, [autoRefresh, refreshInterval, activeEnvironment]);

  // Reset auto-refresh when tab changes or component unmounts
  useEffect(() => {
    const handleVisibilityChange = () => {
      if (document.visibilityState !== 'visible') {
        setAutoRefresh(false);
      }
    };

    document.addEventListener('visibilitychange', handleVisibilityChange);

    return () => {
      document.removeEventListener('visibilitychange', handleVisibilityChange);
      setAutoRefresh(false);
    };
  }, []);

  // Parse port bindings from Docker format
  const parsePortBindings = (portsString: string | undefined): PortBinding[] => {
    if (!portsString || portsString.trim() === '') return [];

    // Common Docker port formats:
    // "0.0.0.0:8080->80/tcp, :::8080->80/tcp"
    // "80/tcp, 443/tcp"

    const portBindings: PortBinding[] = [];

    try {
      const portMappings = portsString.split(', ');

      portMappings.forEach(mapping => {
        // Check if there's a host port mapping
        if (mapping.includes('->')) {
          // Format: "0.0.0.0:8080->80/tcp" or ":::8080->80/tcp"
          const [hostPart, containerPart] = mapping.split('->');

          // Extract the host port, which comes after the last colon
          const hostPort = hostPart.substring(hostPart.lastIndexOf(':') + 1);

          // Extract container port and protocol
          const [containerPort, protocol] = containerPart.split('/');

          portBindings.push({
            hostPort,
            containerPort,
            protocol: protocol || 'tcp'
          });
        } else if (mapping.includes('/')) {
          // Format: "80/tcp" (exposed port without host binding)
          const [containerPort, protocol] = mapping.split('/');

          portBindings.push({
            hostPort: '',
            containerPort,
            protocol: protocol || 'tcp'
          });
        }
      });
    } catch (err) {
      console.error('Error parsing port bindings:', err);
    }

    return portBindings;
  };

  // Load containers from the active environment
  const loadContainers = async () => {
    if (!activeEnvironment) {
      setError('No environment selected');
      return;
    }

    // For initial loading (empty containers list)
    if (composeGroups.length === 0 && ungroupedContainers.length === 0) {
      setIsLoading(true);
    } else {
      setIsRefreshing(true);
    }

    setError('');

    try {
      // Check if Docker Desktop service is available
      if (!ddClient.extension?.vm?.service) {
        throw new Error('Docker Desktop service not available');
      }

      // Make API call to fetch containers
      const response = await ddClient.extension.vm.service.post('/connect', {
        hostname: activeEnvironment.hostname,
        username: activeEnvironment.username,
      });

      // Check for error response
      if (response && typeof response === 'object' && 'error' in response) {
        const errorResponse = response as ErrorResponse;
        throw new Error(errorResponse.error);
      }

      // Cast response to Container array
      const data = response as ContainersResponse;

      // Update states
      setComposeGroups(data.composeGroups);
      setUngroupedContainers(data.ungrouped);

      setLastRefreshTime(new Date()); // Update last refresh time
      console.log('Containers loaded:', data);
    } catch (err: any) {
      console.error('Failed to load containers:', err);
      setError(`Failed to load containers: ${err.message || 'Unknown error'}`);
      setComposeGroups([]);
      setUngroupedContainers([]);
    } finally {
      setIsLoading(false);
      setIsRefreshing(false);
    }
  };

  // Start a container
  const startContainer = async (containerId: string) => {
    if (!activeEnvironment) return;

    setIsRefreshing(true);
    try {
      if (!ddClient.extension?.vm?.service) {
        throw new Error('Docker Desktop service not available');
      }

      const response = await ddClient.extension.vm.service.post('/container/start', {
        hostname: activeEnvironment.hostname,
        username: activeEnvironment.username,
        containerId
      });

      if (response && typeof response === 'object' && 'error' in response) {
        const errorResponse = response as ErrorResponse;
        throw new Error(errorResponse.error);
      }

      // Reload containers after successful operation
      await loadContainers();
    } catch (err: any) {
      console.error('Failed to start container:', err);
      setError(`Failed to start container: ${err.message || 'Unknown error'}`);
      setIsRefreshing(false);
    }
  };

  // Stop a container
  const stopContainer = async (containerId: string) => {
    if (!activeEnvironment) return;

    setIsRefreshing(true);
    try {
      if (!ddClient.extension?.vm?.service) {
        throw new Error('Docker Desktop service not available');
      }

      const response = await ddClient.extension.vm.service.post('/container/stop', {
        hostname: activeEnvironment.hostname,
        username: activeEnvironment.username,
        containerId
      });

      if (response && typeof response === 'object' && 'error' in response) {
        const errorResponse = response as ErrorResponse;
        throw new Error(errorResponse.error);
      }

      // Reload containers after successful operation
      await loadContainers();
    } catch (err: any) {
      console.error('Failed to stop container:', err);
      setError(`Failed to stop container: ${err.message || 'Unknown error'}`);
      setIsRefreshing(false);
    }
  };

  // Check if a container is running
  const isRunning = (status: string): boolean => {
    return status.toLowerCase().includes('up');
  };

  // Auto-refresh handlers
  const handleAutoRefreshChange = (enabled: boolean) => {
    setAutoRefresh(enabled);
  };

  const handleIntervalChange = (interval: number) => {
    setRefreshInterval(interval);
  };

  // View container logs
  const viewContainerLogs = (container: DockerContainer) => {
    setSelectedContainer(container);
    setIsLogsOpen(true); // Update parent component state
  };

  // Close logs drawer
  const closeLogs = () => {
    setIsLogsOpen(false); // Update parent component state
    setSelectedContainer(null);
  };

  // Handle action confirmation
  const handleConfirmAction = () => {
    if (!confirmAction.container) return;

    if (confirmAction.type === 'start') {
      startContainer(confirmAction.container.id);
    } else if (confirmAction.type === 'stop') {
      stopContainer(confirmAction.container.id);
    }

    setConfirmDialogOpen(false);
  };

  // Open confirmation dialog for starting a container
  const confirmStartContainer = (container: DockerContainer) => {
    setConfirmAction({
      type: 'start',
      container: container
    });
    setConfirmDialogOpen(true);
  };

  // Open confirmation dialog for stopping a container
  const confirmStopContainer = (container: DockerContainer) => {
    setConfirmAction({
      type: 'stop',
      container: container
    });
    setConfirmDialogOpen(true);
  };

  // Render port bindings for a container
  const renderPortBindings = (container: DockerContainer) => {
    const portBindings = parsePortBindings(container.ports);

    if (portBindings.length === 0) {
      return <Typography variant="body2" color="text.secondary">None</Typography>;
    }

    return (
      <Stack direction="row" spacing={1} flexWrap="wrap">
        {portBindings.map((binding, index) => (
          <Chip
            key={index}
            size="small"
            icon={<PortIcon />}
            label={
              binding.hostPort
                ? `${binding.hostPort}:${binding.containerPort}/${binding.protocol}`
                : `${binding.containerPort}/${binding.protocol}`
            }
            variant="outlined"
            color="info"
            sx={{ my: 0.5 }}
          />
        ))}
      </Stack>
    );
  };

  // Table for a list of containers (reused for both grouped and ungrouped)
  const renderContainersTable = (containersList: DockerContainer[]) => {
    return (
      <TableContainer component={Paper} sx={{ mb: 3 }}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell width="10%">Container ID</TableCell>
              <TableCell width="15%">Name</TableCell>
              <TableCell width="20%">Image</TableCell>
              <TableCell width="15%">Status</TableCell>
              <TableCell width="30%">Ports</TableCell>
              <TableCell width="10%" align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {containersList.map((container) => (
              <TableRow key={container.id} hover>
                <TableCell sx={{ fontFamily: 'monospace' }}>
                  {container.id.substring(0, 12)}
                </TableCell>
                <TableCell>{container.name}</TableCell>
                <TableCell>{container.image}</TableCell>
                <TableCell>
                  <Chip
                    label={container.status}
                    color={isRunning(container.status) ? 'success' : 'default'}
                    size="small"
                    variant="outlined"
                  />
                </TableCell>
                <TableCell>
                  {renderPortBindings(container)}
                </TableCell>
                <TableCell align="right">
                  <Box sx={{ display: 'flex', justifyContent: 'flex-end' }}>
                    {/* View logs button */}
                    <Tooltip title="View Logs">
                      <IconButton
                        size="small"
                        color="primary"
                        onClick={() => viewContainerLogs(container)}
                        disabled={isRefreshing || isLogsOpen}
                        sx={{ mr: 1 }}
                      >
                        <VisibilityIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>

                    {/* Start/Stop button */}
                    {isRunning(container.status) ? (
                      <Tooltip title="Stop">
                        <IconButton
                          size="small"
                          color="error"
                          onClick={() => confirmStopContainer(container)}
                          disabled={isRefreshing || isLogsOpen}
                        >
                          <StopIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                    ) : (
                      <Tooltip title="Start">
                        <IconButton
                          size="small"
                          color="success"
                          onClick={() => confirmStartContainer(container)}
                          disabled={isRefreshing || isLogsOpen}
                        >
                          <PlayArrowIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                    )}
                  </Box>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    );
  };

  return (
    <Box>
      {/* Header */}
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Typography variant="h5">Containers</Typography>

        <AutoRefreshControls
          autoRefresh={autoRefresh}
          refreshInterval={refreshInterval}
          lastRefreshTime={lastRefreshTime}
          isRefreshing={isRefreshing}
          isDisabled={!activeEnvironment || isLogsOpen}
          onRefreshClick={loadContainers}
          onAutoRefreshChange={handleAutoRefreshChange}
          onIntervalChange={handleIntervalChange}
        />
      </Box>

      {/* Warning when no environment is selected */}
      {!activeEnvironment ? (
        <Alert severity="info" sx={{ mb: 3 }}>
          Please select an environment to view containers.
        </Alert>
      ) : null}

      {/* Error message */}
      {error ? (
        <Alert severity="error" sx={{ mb: 3 }}>
          {error}
        </Alert>
      ) : null}

      {/* Loading indicator for initial load only */}
      {isLoading && composeGroups.length === 0 && ungroupedContainers.length === 0 ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', my: 3 }}>
          <CircularProgress />
        </Box>
      ) : null}

      {/* Refresh overlay while we have data */}
      {(composeGroups.length > 0 || ungroupedContainers.length > 0) && (
        <Box sx={{ position: 'relative' }}>
          {isRefreshing && (
            <Box
              sx={{
                position: 'absolute',
                top: 0,
                left: 0,
                right: 0,
                bottom: 0,
                display: 'flex',
                justifyContent: 'center',
                alignItems: 'center',
                backgroundColor: 'rgba(255, 255, 255, 0.7)',
                zIndex: 1,
                borderRadius: 1
              }}
            >
              <CircularProgress />
            </Box>
          )}

          {/* RENDER COMPOSE GROUPS */}
          {composeGroups.map(group => (
            <Box key={group.name} sx={{ mb: 4 }}>
              <Typography variant="h6" sx={{ mb: 1 }}>
                Compose Project: {group.name}
              </Typography>
              {renderContainersTable(group.containers)}
            </Box>
          ))}

          {/* RENDER UNGROUPED CONTAINERS */}
          {ungroupedContainers.length > 0 && (
            <Box sx={{ mt: 2 }}>
              <Typography variant="h6" sx={{ mb: 1 }}>
                Ungrouped Containers
              </Typography>
              {renderContainersTable(ungroupedContainers)}
            </Box>
          )}

          {/* If no containers at all */}
          {composeGroups.length === 0 && ungroupedContainers.length === 0 && !isLoading && (
            <Alert severity="info">No containers found in the selected environment.</Alert>
          )}
        </Box>
      )}

      {/* Logs drawer */}
      <Drawer
        anchor="bottom"
        open={isLogsOpen}
        onClose={closeLogs}
        sx={{
          '& .MuiDrawer-paper': {
            height: '90%',
            boxShadow: 3,
            borderTopLeftRadius: 8,
            borderTopRightRadius: 8,
          },
        }}
      >
        {selectedContainer && (
          <Box sx={{ p: 2, height: '100%' }}>
            <ContainerLogs
              activeEnvironment={activeEnvironment}
              containerId={selectedContainer.id}
              containerName={selectedContainer.name}
              onClose={closeLogs}
            />
          </Box>
        )}
      </Drawer>

      {/* Confirmation Dialog */}
      <ConfirmationDialog
        open={confirmDialogOpen}
        title={confirmAction.type === 'start' ? 'Start Container' : 'Stop Container'}
        message={
          confirmAction.type === 'start'
            ? 'Are you sure you want to start this container?'
            : 'Are you sure you want to stop this container? Any running processes will be terminated.'
        }
        confirmText={confirmAction.type === 'start' ? 'Start' : 'Stop'}
        confirmColor={confirmAction.type === 'start' ? 'success' : 'error'}
        resourceName={confirmAction.container ? confirmAction.container.name : ''}
        onConfirm={handleConfirmAction}
        onCancel={() => setConfirmDialogOpen(false)}
      />
    </Box>
  );
};

export default Containers;
