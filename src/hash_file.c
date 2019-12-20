#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "bf.h"
#include "hash_file.h"
#define MAX_OPEN_FILES 20

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HT_ERROR;        \
  }                         \
}

int BUCKETS_NUM = 1024;	//same as defined at main
int* table;		//global table
int bucket_num = 1024;	//global variable used to pass number of buckets
	
HT_ErrorCode HT_Init() {
    
  table = malloc(sizeof(int) * MAX_OPEN_FILES);
  return HT_OK;
}

HT_ErrorCode HT_CreateIndex(const char *filename, int buckets) {//create an empty file,HT_init first
  // allocate blocks for map

  CALL_BF(BF_CreateFile(filename));

  int *fd = malloc(sizeof(int));
  CALL_BF(BF_OpenFile(filename, fd));

  //allocate first block,may need more
  BF_Block* block;
  BF_Block_Init(&block);
  CALL_BF(BF_AllocateBlock(*fd, block));
  //start filling block,check for memory

  char* data = BF_Block_GetData(block);//first block is recognition, we store number of buckets in this first block
  memcpy(data,&buckets,4); //apothikeuoume ton arithmo twn buckets pou tha exei auto to index sto prwto block
  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));//no need anymore
  CALL_BF(BF_AllocateBlock(*fd, block));
  data = BF_Block_GetData(block);

  int i = 0;
  int block_numbers = 2; //use it to count blocks which dont store records
  int offset = 0;
  int size = 0;
  for (i=0; i<buckets; i++){
      if ( size + 4 < BF_BLOCK_SIZE ) {
          int x = -1;
          memcpy(data+offset, &x, 4); // 4 digits,offset = 4 to find next bucket
          offset +=  4; //"-1" is a flag to allocate block or not
          size += 4;
      }
      else{ //create new block, maybe error set Null first
          BF_Block_SetDirty(block);//set previous block dirty
          CALL_BF(BF_UnpinBlock(block));//no need anymore
          CALL_BF(BF_AllocateBlock(*fd, block));

          data = BF_Block_GetData(block);
          int x = -1;
          memcpy(data, &x, 4);
          block_numbers++;
          size = 4;
          offset = 4;

      }
  }

  BF_Block_SetDirty(block);//set previous block dirty
  CALL_BF(BF_UnpinBlock(block));//no need anymore
  CALL_BF(BF_CloseFile(*fd));
  BF_Block_Destroy(&block);
  bucket_num = buckets;
  free (fd);
  return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc){
    int fileDesc;
    (*indexDesc)++; // table index for the openfile use as table[indexDesc]
    CALL_BF(BF_OpenFile(fileName,&fileDesc));
    table[*indexDesc] = fileDesc;

    return HT_OK;
}

HT_ErrorCode HT_CloseFile(int indexDesc) {
  CALL_BF(BF_CloseFile(table[indexDesc]));
  return HT_OK;
}


HT_ErrorCode HT_InsertEntry(int indexDesc, Record record) {

  int fd = table[indexDesc];//.fd; //access to file
  
  BF_Block* block; 
  BF_Block_Init(&block);
  CALL_BF(BF_GetBlock(fd,0,block)); 
  

  char * data = BF_Block_GetData(block);//first block is recognition, we store number of buckets in this first block
  memcpy(&bucket_num,data,4); //pairnoume  ton arithmo twn buckets  pou exei auto to index apto prwto block
  CALL_BF(BF_UnpinBlock(block));//no need anymore
  BF_Block_Destroy(&block);
  int bucket = record.id % bucket_num; //hashed value
  int block_num;

  int blockBuckets; //how many buckets a block can store
  static int size = 0;

  BF_Block* new_block; 
  BF_Block_Init(&new_block);
  CALL_BF(BF_GetBlock(fd,1,new_block)); //give 2nd block of the file , this block will contain our mapping 

  blockBuckets = (BF_BLOCK_SIZE / 4) - 1; // minus 1 because of < instead of <=

  char* block_data;
  block_data = BF_Block_GetData(new_block);

  //use map to find bucket,we will use flag

  int numBlock = 1;
  while (blockBuckets <= bucket){
      //block_data += BF_BLOCK_SIZE; // next block
      bucket = bucket - blockBuckets;
      CALL_BF(BF_UnpinBlock(new_block));
      numBlock++;
  }//i am in correct block and i need bucket
  CALL_BF(BF_GetBlock(fd,numBlock,new_block));
  block_data = BF_Block_GetData(new_block);
  block_data = block_data + 4*bucket; //every buckets stores its value in 4 bytes
  // x is the number of 1st bucket's block
  //char str[4];
  int x;
  memcpy(&x,block_data,4);
  //int x = atoi(str);
  int flag = 1;			// FLAG 0 means we don't need to allocate block
  //printf("x is : %d\n",x);
  CALL_BF(BF_UnpinBlock(new_block));
  if (x != -1) { //check for block in the bucket that can store record,if there is no such a block then allocate one
      int y = x; //could be do while
      while ( y != -1 ) { // y = -1 means no more blocks to check for space
          CALL_BF(BF_GetBlock(fd, y, new_block)); // take y-block its number is y-1, may need to unpin
          block_data = BF_Block_GetData(new_block);
          memcpy(&y, block_data, 4);
          //y = atoi(str); // next_block number
                            // printf("y: %d\n",y);

          int r;
          memcpy(&r, block_data + 4, 4);
          //int r = atoi(str);// records number

          //printf("r:      %d\n",r);
          if (r * sizeof(Record) + sizeof(Record) + 8 < BF_BLOCK_SIZE) { // 8 bytes for next block and records number
              flag = 0;			// new record fits in this block, so we don't need to allocate new block, so we mark flag = 0
              //CALL_BF(BF_UnpinBlock(new_block));
              break;
          }
          CALL_BF(BF_UnpinBlock(new_block));
      }
  }
  if ( x == -1 || flag == 1) { //allocate , x is block's number in the file			
      BF_Block *next_block;
      BF_Block_Init(&next_block);
      CALL_BF(BF_AllocateBlock(fd, next_block));
      int *n = malloc(sizeof(int));
      CALL_BF(BF_GetBlockCounter(fd, n));
      //char c[5];
      //sprintf(c, "%d", *n - 1);
      int c = *n - 1;

      //printf("block number n: %d \n",c);
      memcpy(block_data, &c, 4); // next block
      block_data = BF_Block_GetData(next_block);

      int temp = -1;
      memcpy(block_data,&temp,4); //-1 means there is no next_block
      temp = 1;
      memcpy(block_data+4,&temp,4); //store how many records in Block,could use static table to store it
      block_data += 8; // 8 first bytes are for next block and records number

      //sprintf(c, "%d", record.id); //c is the string to store id

      memcpy(block_data,&record,sizeof(Record));

      BF_Block_SetDirty(new_block);
      CALL_BF(BF_UnpinBlock(new_block));

      BF_Block_SetDirty(next_block);//we wrote so its dirty
      CALL_BF(BF_UnpinBlock(next_block));//no need anymore
      BF_Block_Destroy(&next_block);
      free (n);
  }
  else{
      int blockRecords;
      memcpy(&blockRecords, block_data+4, 4);
      //int blockRecords = atoi(str); //number of records
      //char c[5];

      //sprintf(c, "%d", blockRecords+1);
      int c = blockRecords + 1;
      BF_Block_SetDirty(new_block);
      memcpy(block_data+4, &c, 4); //add 1 record
      //add 8 because 8 bytes are used for next_block and records number
      block_data += 8;
      //sprintf(c, "%d", record.id); //c is the string to store id

      memcpy(block_data + blockRecords * sizeof(Record),&record,sizeof(Record));

      BF_Block_SetDirty(new_block);
      CALL_BF(BF_UnpinBlock(new_block));
  }
  BF_Block_Destroy(&new_block);
  return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
  int fd = table[indexDesc];

  if ( id != NULL ) {
      int bucket = *id % BUCKETS_NUM; //BUCKETS_NUM;
      int blockBuckets = (BF_BLOCK_SIZE / 4) - 1; //how many buckets a block can store

      BF_Block *new_block;
      BF_Block_Init(&new_block);
      CALL_BF(BF_GetBlock(fd,1,new_block));

      char *block_data;
      block_data = BF_Block_GetData(new_block);

      //use map to find bucket
      int numBlock = 1;
      while (blockBuckets <= bucket) {
          block_data += BF_BLOCK_SIZE; // next block
          bucket = bucket - blockBuckets;
          CALL_BF(BF_UnpinBlock(new_block));
          numBlock++;
      }//i am in correct block and i need bucket
      CALL_BF(BF_GetBlock(fd,numBlock,new_block));
      block_data = BF_Block_GetData(new_block);
      block_data = block_data + 4 * bucket; //every buckets stores its value in 4 bytes

      char str[4];
      int x;
      memcpy(&x, block_data, 4);
      //int x = atoi(str); // block's number that this record would be stored
      if (x == -1) { CALL_BF(BF_UnpinBlock(new_block)); BF_Block_Destroy(&new_block); printf("not found\n"); return HT_OK; }
      CALL_BF(BF_UnpinBlock(new_block));
      CALL_BF(BF_GetBlock(table[indexDesc], x, new_block)); // take x-block its number is x-1, may need to unpin
      block_data = BF_Block_GetData(new_block);

      int nextBlock;
      memcpy(&nextBlock, block_data, 4);
      //int nextBlock = atoi(str); // next_block number
      int recordsNum;
      memcpy(&recordsNum, block_data+4, 4);
      //int recordsNum = atoi(str);
      block_data += 8; // i am at first record
      //search records of this block
      int i;
      int record_id;
      do {
          for (i = 0; i < recordsNum; i++) { //compile may throw a warning,could make str[8] to solve this
              memcpy(&record_id, block_data, 4); //assume the int can be stored in 4 bytes since the id will be 4 digits maximum
              if (*id == record_id) {
                  printf("id: %d name: %s surname: %s city: %s\n", *id, block_data+4, block_data+19, block_data+39);
                  CALL_BF(BF_UnpinBlock(new_block));
                  BF_Block_Destroy(&new_block);

                  return HT_OK;
              }
              block_data += sizeof(Record); //sizeof(record)
          }
          CALL_BF(BF_UnpinBlock(new_block));
          CALL_BF(BF_GetBlock(table[indexDesc], nextBlock, new_block));
          block_data = BF_Block_GetData(new_block);


          //set starting values for the new block
          memcpy(&nextBlock, block_data, 4);
          //nextBlock = atoi(str); // next_block number
          CALL_BF(BF_UnpinBlock(new_block));
          if(nextBlock == -1) {       BF_Block_Destroy(&new_block); printf("not found\n"); return HT_OK; } //not found
          memcpy(&recordsNum, block_data+4, 4);
          //recordsNum = atoi(str);
          block_data += 8; // i am at first record

      } while (nextBlock != -1);
      
  }
  else{

      BF_Block *new_block;
      BF_Block_Init(&new_block);
      CALL_BF(BF_GetBlock(fd,1,new_block));

      char *block_data;
      block_data = BF_Block_GetData(new_block);

      //CALL_BF(BF_UnpinBlock(new_block));

      int bucket_num = BUCKETS_NUM;
      int countR = 0;
      int counter = 0;
      int mapBlock = 1;
      while(bucket_num != 0){
          //char str[4];
          counter++;
          int temp;
          memcpy(&temp, block_data, 4);
         // printf("bucket points %d\n",temp);
          if ( temp != -1){
            BF_Block *new_blockB; // bucket's blocks
            BF_Block_Init(&new_blockB);
            CALL_BF(BF_GetBlock(fd, temp, new_blockB));

            char *block_dataB;
            block_dataB = BF_Block_GetData(new_blockB);

            //char strB[4];
            //search records of this block
            int i;
            int record_id;
            int nextBlock;
            do { //print bucket's records for all of its blocks
                //set starting values for the new block
                memcpy(&nextBlock, block_dataB, 4);
                //nextBlock = atoi(strB); // next_block number
                //printf("nB %d\n",nextBlock);
                int recordsNum;
                memcpy(&recordsNum, block_dataB + 4, 4);
                //int recordsNum = atoi(strB);
                block_dataB += 8; // i am at first record
                countR += recordsNum;
                for (i = 0; i < recordsNum; i++) { //compile may throw a warning,could make str[8] to solve this
                    memcpy(&record_id, block_dataB, 4); //assume the int can be stored in 4 bytes since the id will be 4 digits maximum
                    printf("id: %d name: %s surname: %s city: %s\n", record_id, block_dataB+4, block_dataB+19, block_dataB+39);
                    block_dataB += sizeof(Record); //sizeof(record)
                }

                CALL_BF(BF_UnpinBlock(new_blockB));
                if(nextBlock == -1) break; //not found
                CALL_BF(BF_GetBlock(table[indexDesc], nextBlock, new_blockB));
                block_dataB = BF_Block_GetData(new_blockB);
                CALL_BF(BF_UnpinBlock(new_blockB));
            } while (nextBlock != -1);
            BF_Block_Destroy(&new_blockB);
          }
          bucket_num--; //find next bucket
          block_data += 4;
          if (counter == (BF_BLOCK_SIZE/4 -1)) {
               counter = 0;
               CALL_BF(BF_UnpinBlock(new_block));
               if (bucket_num == 0) break;
               CALL_BF(BF_GetBlock(fd,++mapBlock,new_block));
               block_data = BF_Block_GetData(new_block);
          }//we need to move to map's next block
      }
      printf("countR: %d\n",countR);
      BF_Block_Destroy(&new_block);
  }

  return HT_OK;
}

HT_ErrorCode HT_DeleteEntry(int indexDesc, int id) {
    int fd = table[indexDesc];

    if ( id != -10 ) {
        int bucket = id % BUCKETS_NUM; //BUCKETS_NUM;
        int blockBuckets = (BF_BLOCK_SIZE / 4) - 1; //how many buckets a block can store

        BF_Block *new_block;
        BF_Block_Init(&new_block);
        CALL_BF(BF_GetBlock(fd,1,new_block));

        char *block_data;
        block_data = BF_Block_GetData(new_block);

        CALL_BF(BF_UnpinBlock(new_block));
        //use map to find bucket
        /*while (blockBuckets < bucket) {
            block_data += BF_BLOCK_SIZE; // next block
            bucket = bucket - blockBuckets;
        }//i am in correct block and i need bucket
        block_data = block_data + 4 * bucket; //every buckets stores its value in 4 bytes */


      int numBlock = 1;
      while (blockBuckets <= bucket) {
          block_data += BF_BLOCK_SIZE; // next block
          bucket = bucket - blockBuckets;
          CALL_BF(BF_UnpinBlock(new_block));
          numBlock++;
      }//i am in correct block and i need bucket
      CALL_BF(BF_GetBlock(fd,numBlock,new_block));
      block_data = BF_Block_GetData(new_block);
      block_data = block_data + 4 * bucket;
        //char str[4];
        int x;
        memcpy(&x, block_data, 4);
        //int x = atoi(str); // block's number that this record would be stored
        if (x == -1) { BF_Block_Destroy(&new_block); printf("not found\n"); return HT_OK; }

        CALL_BF(BF_GetBlock(table[indexDesc], x, new_block)); // take x-block its number is x-1, may need to unpin
        block_data = BF_Block_GetData(new_block);

        int nextBlock;
        memcpy(&nextBlock, block_data, 4);
        //int nextBlock = atoi(str); // next_block number
        int recordsNum;
        memcpy(&recordsNum, block_data+4, 4);
        //int recordsNum = atoi(str);
        block_data += 8; // i am at first record
        //search records of this block
        char* block_data2 = block_data;
        block_data2 = block_data + (recordsNum - 1) * sizeof(Record);
        int i;
        int record_id;
        do {
            for (i = 0; i < recordsNum; i++) { //compile may throw a warning,could make str[8] to solve this
                memcpy(&record_id, block_data, 4); //assume the int can be stored in 4 bytes since the id will be 4 digits maximum
                if (id == record_id) {
                    //char c[4];
                    //sprintf(c, "%d", recordsNum-1);
                    int temp = recordsNum-1;
                    memcpy(block_data-4,&temp,4);
                    printf("just deleted id: %d name: %s surname: %s city: %s\n", id, block_data+4, block_data+19, block_data+39);
                    if (recordsNum == 1) {CALL_BF(BF_UnpinBlock(new_block)); BF_Block_Destroy(&new_block); return HT_OK; }
                    memcpy(block_data, block_data2, sizeof(Record));
                    CALL_BF(BF_UnpinBlock(new_block));
                    BF_Block_Destroy(&new_block);
                    return HT_OK;
                }
                block_data += sizeof(Record); //sizeof(record)
            }
            if(nextBlock == -1) {CALL_BF(BF_UnpinBlock(new_block)); BF_Block_Destroy(&new_block); printf("not found\n"); return HT_OK; } //not found
            CALL_BF(BF_UnpinBlock(new_block));

            CALL_BF(BF_GetBlock(table[indexDesc], nextBlock, new_block));
            block_data = BF_Block_GetData(new_block);

            //set starting values for the new block
            memcpy(&nextBlock, block_data, 4);
            //nextBlock = atoi(str); // next_block number
            memcpy(&recordsNum, block_data+4, 4);
            //recordsNum = atoi(str);
            block_data += 8; // i am at first record
        } while (nextBlock != -1);
        BF_Block_Destroy(&new_block);
    }
  return HT_OK;
}
