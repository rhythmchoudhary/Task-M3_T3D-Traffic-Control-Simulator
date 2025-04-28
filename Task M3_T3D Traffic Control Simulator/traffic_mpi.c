#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_LINE_LENGTH 128
#define MAX_LIGHTS 100
#define HOURS_IN_DAY 24
#define TOP_N 3

typedef struct {
    char light_id[10];
    int count;
} LightData;

typedef struct {
    LightData lights[MAX_LIGHTS];
    int num_lights;
} HourData;

HourData traffic_data[HOURS_IN_DAY];

// Function to get hour from timestamp
int get_hour(const char* timestamp) {
    int hour = 0;
    sscanf(timestamp, "%d", &hour);
    return hour;
}

// Add traffic data
void add_traffic(const char* light_id, int hour, int cars) {
    HourData* hd = &traffic_data[hour];
    for (int i = 0; i < hd->num_lights; i++) {
        if (strcmp(hd->lights[i].light_id, light_id) == 0) {
            hd->lights[i].count += cars;
            return;
        }
    }
    // New light
    strcpy(hd->lights[hd->num_lights].light_id, light_id);
    hd->lights[hd->num_lights].count = cars;
    hd->num_lights++;
}

// Merge data from worker
void merge_data(HourData* worker_data) {
    for (int h = 0; h < HOURS_IN_DAY; h++) {
        HourData* whd = &worker_data[h];
        for (int i = 0; i < whd->num_lights; i++) {
            add_traffic(whd->lights[i].light_id, h, whd->lights[i].count);
        }
    }
}

// Compare function for sorting
int compare(const void* a, const void* b) {
    return ((LightData*)b)->count - ((LightData*)a)->count;
}

// Display top congested lights
void display_top_congested() {
    printf("\nTop Congested Traffic Lights Per Hour:\n");
    for (int h = 0; h < HOURS_IN_DAY; h++) {
        HourData* hd = &traffic_data[h];
        if (hd->num_lights > 0) {
            qsort(hd->lights, hd->num_lights, sizeof(LightData), compare);
            printf("Hour %02d:00\n", h);
            for (int i = 0; i < TOP_N && i < hd->num_lights; i++) {
                printf("  %s: %d cars\n", hd->lights[i].light_id, hd->lights[i].count);
            }
        }
    }
}

int main(int argc, char** argv) {
    int rank, size;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (argc < 2) {
        if (rank == 0) {
            printf("Usage: %s <input_file>\n", argv[0]);
        }
        MPI_Finalize();
        return 0;
    }

    if (rank == 0) {
        // Master
        FILE* file = fopen(argv[1], "r");
        if (!file) {
            printf("Failed to open file.\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        char lines[1000][MAX_LINE_LENGTH];
        int total_lines = 0;
        while (fgets(lines[total_lines], MAX_LINE_LENGTH, file)) {
            if (strlen(lines[total_lines]) > 1) {
                total_lines++;
            }
        }
        fclose(file);

        int chunk = total_lines / (size - 1);
        int extra = total_lines % (size - 1);

        // Send data chunks
        int offset = 0;
        for (int i = 1; i < size; i++) {
            int count = chunk + (i <= extra ? 1 : 0);
            MPI_Send(&count, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
            MPI_Send(lines + offset, count * MAX_LINE_LENGTH, MPI_CHAR, i, 0, MPI_COMM_WORLD);
            offset += count;
        }

        // Receive processed results
        for (int i = 1; i < size; i++) {
            HourData worker_data[HOURS_IN_DAY];
            MPI_Recv(worker_data, HOURS_IN_DAY * sizeof(HourData), MPI_BYTE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            merge_data(worker_data);
        }

        display_top_congested();

    } else {
        // Worker
        int count;
        MPI_Recv(&count, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        char* recv_lines = malloc(count * MAX_LINE_LENGTH * sizeof(char));
        MPI_Recv(recv_lines, count * MAX_LINE_LENGTH, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // Process lines
        for (int i = 0; i < count; i++) {
            char timestamp[10], light_id[10];
            int cars;
            sscanf(recv_lines + i * MAX_LINE_LENGTH, "%s %s %d", timestamp, light_id, &cars);
            int hour = get_hour(timestamp);
            add_traffic(light_id, hour, cars);
        }

        free(recv_lines);

        // Send results back
        MPI_Send(traffic_data, HOURS_IN_DAY * sizeof(HourData), MPI_BYTE, 0, 0, MPI_COMM_WORLD);
    }

    MPI_Finalize();
    return 0;
}
