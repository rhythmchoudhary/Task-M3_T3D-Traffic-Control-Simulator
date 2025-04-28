// Include necessary libraries
#include <mpi.h>          // For MPI functions (parallel programming)
#include <stdio.h>        // For input and output functions
#include <stdlib.h>       // For memory allocation
#include <string.h>       // For string handling functions

// Define constants
#define MAX_LINE_LENGTH 128  // Maximum length of each input line
#define MAX_LIGHTS 100       // Maximum number of traffic lights
#define HOURS_IN_DAY 24      // Total hours in a day
#define TOP_N 3              // Top 3 most congested lights to show

// Structure to hold one light's data (light id and car count)
typedef struct {
    char light_id[10];
    int count;
} LightData;

// Structure to hold data for one hour (all lights in that hour)
typedef struct {
    LightData lights[MAX_LIGHTS];
    int num_lights;
} HourData;

// Array to hold traffic data for all 24 hours
HourData traffic_data[HOURS_IN_DAY];

// Function to extract hour from timestamp string
int get_hour(const char* timestamp) {
    int hour = 0;
    sscanf(timestamp, "%d", &hour);  // Read first number from timestamp (hour part)
    return hour;
}

// Function to add new traffic record or update existing one
void add_traffic(const char* light_id, int hour, int cars) {
    HourData* hd = &traffic_data[hour];  // Get data for the given hour
    for (int i = 0; i < hd->num_lights; i++) {
        // If light id matches existing one, just add cars
        if (strcmp(hd->lights[i].light_id, light_id) == 0) {
            hd->lights[i].count += cars;
            return;
        }
    }
    // If new light id, create a new entry
    strcpy(hd->lights[hd->num_lights].light_id, light_id);
    hd->lights[hd->num_lights].count = cars;
    hd->num_lights++;
}

// Function to merge worker's processed data into master's data
void merge_data(HourData* worker_data) {
    for (int h = 0; h < HOURS_IN_DAY; h++) {
        HourData* whd = &worker_data[h];
        for (int i = 0; i < whd->num_lights; i++) {
            add_traffic(whd->lights[i].light_id, h, whd->lights[i].count);
        }
    }
}

// Function used for sorting lights (highest car count first)
int compare(const void* a, const void* b) {
    return ((LightData*)b)->count - ((LightData*)a)->count;
}

// Function to display top 3 congested lights for each hour
void display_top_congested() {
    printf("\nTop Congested Traffic Lights Per Hour:\n");
    for (int h = 0; h < HOURS_IN_DAY; h++) {
        HourData* hd = &traffic_data[h];
        if (hd->num_lights > 0) {
            // Sort lights for the current hour
            qsort(hd->lights, hd->num_lights, sizeof(LightData), compare);
            printf("Hour %02d:00\n", h);
            // Print top 3 or less if not enough lights
            for (int i = 0; i < TOP_N && i < hd->num_lights; i++) {
                printf("  %s: %d cars\n", hd->lights[i].light_id, hd->lights[i].count);
            }
        }
    }
}

int main(int argc, char** argv) {
    int rank, size;

    // Initialize MPI environment
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);  // Get this process's rank (id)
    MPI_Comm_size(MPI_COMM_WORLD, &size);  // Get total number of processes

    // If input file not provided, exit
    if (argc < 2) {
        if (rank == 0) {
            printf("Usage: %s <input_file>\n", argv[0]);
        }
        MPI_Finalize();
        return 0;
    }

    // MASTER CODE (rank 0)
    if (rank == 0) {
        // Open input file
        FILE* file = fopen(argv[1], "r");
        if (!file) {
            printf("Failed to open file.\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        // Read all lines from the file into an array
        char lines[1000][MAX_LINE_LENGTH];
        int total_lines = 0;
        while (fgets(lines[total_lines], MAX_LINE_LENGTH, file)) {
            if (strlen(lines[total_lines]) > 1) { // Ignore empty lines
                total_lines++;
            }
        }
        fclose(file);

        // Decide how many lines each worker will get
        int chunk = total_lines / (size - 1);  // Equal lines per worker
        int extra = total_lines % (size - 1);  // Leftover lines

        // Send data to workers
        int offset = 0;
        for (int i = 1; i < size; i++) {
            int count = chunk + (i <= extra ? 1 : 0);  // Distribute extra lines too
            MPI_Send(&count, 1, MPI_INT, i, 0, MPI_COMM_WORLD); // Send number of lines
            MPI_Send(lines + offset, count * MAX_LINE_LENGTH, MPI_CHAR, i, 0, MPI_COMM_WORLD); // Send actual lines
            offset += count;
        }

        // Receive processed traffic data from all workers
        for (int i = 1; i < size; i++) {
            HourData worker_data[HOURS_IN_DAY];
            MPI_Recv(worker_data, HOURS_IN_DAY * sizeof(HourData), MPI_BYTE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            merge_data(worker_data);  // Merge each worker's data into master's data
        }

        // After collecting all results, display top congested lights
        display_top_congested();

    } 
    
    // WORKER CODE (rank > 0)
    else {
        int count;

        // Receive how many lines this worker has to process
        MPI_Recv(&count, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        // Allocate memory to receive lines
        char* recv_lines = malloc(count * MAX_LINE_LENGTH * sizeof(char));
        MPI_Recv(recv_lines, count * MAX_LINE_LENGTH, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // Process each received line
        for (int i = 0; i < count; i++) {
            char timestamp[10], light_id[10];
            int cars;
            // Parse timestamp, light id, and car count from the line
            sscanf(recv_lines + i * MAX_LINE_LENGTH, "%s %s %d", timestamp, light_id, &cars);
            int hour = get_hour(timestamp);  // Get hour from timestamp
            add_traffic(light_id, hour, cars);  // Add this record to local traffic data
        }

        free(recv_lines);  // Free memory

        // Send processed data back to master
        MPI_Send(traffic_data, HOURS_IN_DAY * sizeof(HourData), MPI_BYTE, 0, 0, MPI_COMM_WORLD);
    }

    // Finalize MPI environment
    MPI_Finalize();
    return 0;
}
