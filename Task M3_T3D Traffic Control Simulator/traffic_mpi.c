#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_LINE_LENGTH 128
#define MAX_LIGHTS 100
#define MAX_DAYS 10
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

typedef struct {
    char date[11];
    HourData hourly[HOURS_IN_DAY];
} DayData;

DayData traffic_data[MAX_DAYS];
int total_days = 0;

// Get hour from HH:MM format
int get_hour(const char* time) {
    int hour;
    sscanf(time, "%d", &hour);
    return hour;
}

// Get or create index for a date
int get_day_index(const char* date) {
    for (int i = 0; i < total_days; i++) {
        if (strcmp(traffic_data[i].date, date) == 0)
            return i;
    }
    // New date
    strcpy(traffic_data[total_days].date, date);
    return total_days++;
}

// Add traffic data
void add_traffic(const char* date, int hour, const char* light_id, int cars) {
    int day_idx = get_day_index(date);
    HourData* hd = &traffic_data[day_idx].hourly[hour];
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

// Merge from worker data
void merge_data(DayData* worker_data, int worker_days) {
    for (int d = 0; d < worker_days; d++) {
        int day_idx = get_day_index(worker_data[d].date);
        for (int h = 0; h < HOURS_IN_DAY; h++) {
            HourData* src = &worker_data[d].hourly[h];
            for (int i = 0; i < src->num_lights; i++) {
                add_traffic(worker_data[d].date, h, src->lights[i].light_id, src->lights[i].count);
            }
        }
    }
}

int compare(const void* a, const void* b) {
    return ((LightData*)b)->count - ((LightData*)a)->count;
}

void display_top_congested() {
    printf("\nTop %d Congested Traffic Lights Per Hour:\n", TOP_N);
    for (int d = 0; d < total_days; d++) {
        printf("\nDate: %s\n", traffic_data[d].date);
        for (int h = 0; h < HOURS_IN_DAY; h++) {
            HourData* hd = &traffic_data[d].hourly[h];
            if (hd->num_lights > 0) {
                qsort(hd->lights, hd->num_lights, sizeof(LightData), compare);
                printf("  Hour %02d:00\n", h);
                for (int i = 0; i < TOP_N && i < hd->num_lights; i++) {
                    printf("    %s: %d cars\n", hd->lights[i].light_id, hd->lights[i].count);
                }
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
        if (rank == 0)
            printf("Usage: %s <input_file>\n", argv[0]);
        MPI_Finalize();
        return 0;
    }

    if (rank == 0) {
        FILE* file = fopen(argv[1], "r");
        if (!file) {
            printf("Cannot open input file.\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        char lines[1000][MAX_LINE_LENGTH];
        int total_lines = 0;
        while (fgets(lines[total_lines], MAX_LINE_LENGTH, file)) {
            if (strlen(lines[total_lines]) > 1)
                total_lines++;
        }
        fclose(file);

        int chunk = total_lines / (size - 1);
        int extra = total_lines % (size - 1);
        int offset = 0;

        for (int i = 1; i < size; i++) {
            int count = chunk + (i <= extra ? 1 : 0);
            MPI_Send(&count, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
            MPI_Send(lines + offset, count * MAX_LINE_LENGTH, MPI_CHAR, i, 0, MPI_COMM_WORLD);
            offset += count;
        }

        for (int i = 1; i < size; i++) {
            int worker_days;
            MPI_Recv(&worker_days, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            DayData worker_data[MAX_DAYS];
            MPI_Recv(worker_data, worker_days * sizeof(DayData), MPI_BYTE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            merge_data(worker_data, worker_days);
        }

        display_top_congested();
    } else {
        int count;
        MPI_Recv(&count, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        char* recv_lines = malloc(count * MAX_LINE_LENGTH);
        MPI_Recv(recv_lines, count * MAX_LINE_LENGTH, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        DayData worker_data[MAX_DAYS];
        int worker_days = 0;

        for (int i = 0; i < count; i++) {
            char date[11], time[6], light_id[10];
            int cars;
            sscanf(recv_lines + i * MAX_LINE_LENGTH, "%10s %5s %s %d", date, time, light_id, &cars);
            int hour = get_hour(time);

            int found = 0;
            int idx = 0;
            for (int d = 0; d < worker_days; d++) {
                if (strcmp(worker_data[d].date, date) == 0) {
                    found = 1;
                    idx = d;
                    break;
                }
            }
            if (!found) {
                strcpy(worker_data[worker_days].date, date);
                idx = worker_days++;
            }

            HourData* hd = &worker_data[idx].hourly[hour];
            int j;
            for (j = 0; j < hd->num_lights; j++) {
                if (strcmp(hd->lights[j].light_id, light_id) == 0) {
                    hd->lights[j].count += cars;
                    break;
                }
            }
            if (j == hd->num_lights) {
                strcpy(hd->lights[j].light_id, light_id);
                hd->lights[j].count = cars;
                hd->num_lights++;
            }
        }

        free(recv_lines);
        MPI_Send(&worker_days, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        MPI_Send(worker_data, worker_days * sizeof(DayData), MPI_BYTE, 0, 0, MPI_COMM_WORLD);
    }

    MPI_Finalize();
    return 0;
}
