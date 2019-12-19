#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "hash_file.h"

#define RECORDS_NUM 1700 // you can change it if you want
#define BUCKETS_NUM 13  // you can change it if you want
#define FILE_NAME "data.db"

const char* names[] = {
  "Yannis",
  "Christofos",
  "Sofia",
  "Marianna",
  "Vagelis",
  "Maria",
  "Iosif",
  "Dionisis",
  "Konstantina",
  "Theofilos",
  "Giorgos",
  "Dimitris"
};

const char* surnames[] = {
  "Ioannidis",
  "Svingos",
  "Karvounari",
  "Rezkalla",
  "Nikolopoulos",
  "Berreta",
  "Koronis",
  "Gaitanis",
  "Oikonomou",
  "Mailis",
  "Michas",
  "Halatsis"
};

const char* cities[] = {
  "Athens",
  "San Francisco",
  "Los Angeles",
  "Amsterdam",
  "London",
  "New York",
  "Tokyo",
  "Hong Kong",
  "Munich",
  "Miami"
};

#define CALL_OR_DIE(call)     \
  {                           \
    HT_ErrorCode code = call; \
    if (code != HT_OK) {      \
      printf("Error\n");      \
      exit(code);             \
    }                         \
  }

extern int *table;

int main() {
    //VERSION WITH MULTIPLE INDEX FILES, with a custom number of buckets
  BF_Init(LRU);

  CALL_OR_DIE(HT_Init());

  int indexDesc = -1;
  CALL_OR_DIE(HT_CreateIndex("data1.db", BUCKETS_NUM));
  CALL_OR_DIE(HT_OpenIndex("data1.db", &indexDesc));
  CALL_OR_DIE(HT_CreateIndex("data2.db", BUCKETS_NUM));
  CALL_OR_DIE(HT_OpenIndex("data2.db", &indexDesc));
  CALL_OR_DIE(HT_CreateIndex("data3.db", BUCKETS_NUM));
  CALL_OR_DIE(HT_OpenIndex("data3.db", &indexDesc));


  Record record;
  srand(12569874);
  int r;


  printf("Insert Entries\n");
  for (int i = 0; i <= indexDesc; i++) {
      for (int id = 0; id < RECORDS_NUM; ++id) {
          record.id = id;
          r = rand() % 12;
          memcpy(record.name, names[r], strlen(names[r]) + 1);
          r = rand() % 12;
          memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
          r = rand() % 10;
          memcpy(record.city, cities[r], strlen(cities[r]) + 1);
          CALL_OR_DIE(HT_InsertEntry(i, record));
      }
      int id = rand() % RECORDS_NUM ;
      CALL_OR_DIE(HT_PrintAllEntries(i, &id));

      //printf("Delete Entry with id = %d\n", id);
      //CALL_OR_DIE(HT_DeleteEntry(i, id));
      printf("\n");

      printf("Print all entries from file in table[%d]\n",i);
      CALL_OR_DIE(HT_PrintAllEntries(i, NULL));
      printf("\n\n -------------------------------------\n\n");
  }


  for (int i = 0; i <= indexDesc; i++) {
    CALL_OR_DIE(HT_CloseFile(i));
  }


  BF_Close();
  free(table);
}
