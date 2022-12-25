#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#define MAX_BUCKET 200   // the number of buckets used
#define RANGE_BUCKET 100000000  // how many nodes each bucket can contain
// #define MAX_NUMBERS 200000000 // the total amount of random number


//the node element used in the bucket
// the data is the value, all the elements in the bucket will form  a linked list
typedef struct _node {
    // int data;
    struct entry* data;
    struct _node * next;
}node;

// thread_id, and the head of the list of current bucket in the thread
struct thread_data{
   int  thread_id;// thread ID
    node *in; //the head of the list
};

typedef struct entry
{
    int key;
    int* record;
}entry;

struct entry *entryArray;




//each bucket will use one thread, therefore we have to define MAX_BUCKET thread
pthread_t threads[MAX_BUCKET];

//all the threads
struct thread_data thread_data_array[MAX_BUCKET];

pthread_mutex_t lockBucket;

//the main sort function
int* bucket_sort(struct entry* array,int arraySize, char* dst);

//the sort in each bucket
void *thread_bucket_sort(void *in);

//the sort method used for individual bucket
node *insert_sort(node *list);

//decide which bucket to put for a data
int returnBucketIndex(int data);

unsigned int byte_to_int (unsigned char* buffer){
	return (buffer[0] << 24) + (buffer[1] << 16) + (buffer[2] << 8) + buffer[3];
}

node* insertionSortList(node* head);
void insertNode(node* root, node* insert);
void deleteNode(node* before);

node* mergeSortList(node* head);
node* sortList(node* head, node* tail);
node* merge(node* head1, node* head2);


//initial bucket to hold all the data
node ** all_buckets;
// it will hold the sorted bucket 
node ** all_buckets_t;

    

int* bucket_sort(struct entry* array, int arraySize, char* dst)
{
    // printf("%s, initial array value\n",array);
    // printf("start bucket sort\n");
    int i,j;
    int* outputArray  = malloc(sizeof(int) * arraySize * 100);
    // printf("size of array : %ld \n", (sizeof(int) * arraySize * 100));
    
    all_buckets = (node **)malloc(sizeof(node *)*MAX_BUCKET);
    all_buckets_t = (node **)malloc(sizeof(node *)*MAX_BUCKET);

    for(i = 0; i<MAX_BUCKET; i++)
    {
        all_buckets[i] = NULL;
        all_buckets_t[i] = NULL;
    }

    // put all the data in the array to the bucket
    for(i = 0; i<arraySize; i++)
    {
        node *cur;
        int bucket_index = returnBucketIndex(array[i].key);
        // printf("after index\n");
        cur = (node *)malloc(sizeof(node));
        cur->data = &array[i];
        // printf("array %d : %d, value99 : %d \n",i, array[i].key, cur->data->record[95]);
        cur->next = all_buckets[bucket_index];
        // printf("all bucket\n");
        all_buckets[bucket_index] = cur;
        // printf("%d \n", i);
    }
    // printf("start put data into bucket\n");
    //print all the data in the bucket
    for(i = 0; i<MAX_BUCKET; i++)
    {
        node *temp = all_buckets[i];
        if(temp != NULL)
        {
            // printf("this is the %d th bucket\n",i);
            while(temp!=NULL)
            {
                // printf("%d ",temp->data->key);
                temp = temp->next;
            }
            // printf("\n");
        }
    }

    // printf("\n");
    // printf("divider\n");
    // printf("\n");
    // printf("start sorting \n");
    //sort for individual bucket
    for(i = 0; i<MAX_BUCKET; i++)
    {
        thread_data_array[i].thread_id = i;
        thread_data_array[i].in = all_buckets[i];
        int rc = pthread_create(&threads[i], NULL, thread_bucket_sort, (void *) &thread_data_array[i]);
        // rc return 0 when it succeses, other when it failed
        if (rc)
        {
            // printf("ERROR; return code from pthread_create() is %d\n", rc);
            return NULL;
        }
    }
    
    for(i=0; i<MAX_BUCKET; i++)
    {
        int rc = pthread_join(threads[i], NULL);
        if (rc)
        {
            // printf("ERROR; return code from pthread_join() is %d\n", rc);
            return NULL;
        }
    }

    //print the sorted array
    //single thread rewrite, could change to multi-thread
    // printf("\n the sorted array is \n");
    //put the data inside the bucket back
    j= 0;

    for(i = (MAX_BUCKET/2)-1; i >=0; i--){
        node *temp_node = all_buckets_t[i];
        while(temp_node != NULL)
        {
            // array[j++] = *(temp_node->data);
            // dst[j * 25] = (temp_node->data->key);
            memcpy(dst +j*100, &temp_node->data->key, 4);
            //void *memcpy(void *dest, const void * src, size_t n)
            // printf("pointer %p, pointer+1 %p \n", outputArray, outputArray+1);
            memcpy(dst +j*100+4, temp_node->data->record, 96);
            // printf("%d, %d  \n", temp_node->data->key, temp_node->data->record[0]);
            temp_node = temp_node->next;
            j++;
        } 
    }

    for(i = MAX_BUCKET/2; i <MAX_BUCKET; i++){
        node *temp_node = all_buckets_t[i];
        while(temp_node != NULL)
        {
            // array[j++] = *(temp_node->data);
            // dst[j * 25] = (temp_node->data->key);
            memcpy(dst +j*100, &temp_node->data->key, 4);
            //void *memcpy(void *dest, const void * src, size_t n)
            // printf("pointer %p, pointer+1 %p \n", outputArray, outputArray+1);
            memcpy(dst +j*100+4, temp_node->data->record, 96);
            // printf("%d, %d  \n", temp_node->data->key, temp_node->data->record[0]);
            temp_node = temp_node->next;
            j++;
        }
    }

    // printf("\n");

    // printf("\nthe length of the bucket is %d\n\n\n",j);

    // for(int i = 0; i<arraySize; i++){
        // printf("key  %d , first value %d \n", outputArray[i*25], outputArray[i*25 + 1]);
    // }
    // printf("before return\n");
    return outputArray;
    
    //release the memory
    // for(i = 0; i<MAX_BUCKET; i++)
    // {
    //     node *head = all_buckets[i];
    //     while(head != NULL)
    //     {
    //         node *temp;
    //         temp = head;
    //         head = head->next;
    //         free(temp);
    //     }
    // }
    free(all_buckets_t);
    free(all_buckets);
    free(entryArray);
}

//the sort at each thread
void *thread_bucket_sort(void *in)
{
    struct thread_data *strcut_data = (struct thread_data *)in;
    node *list = (strcut_data->in);    //the start node of each thread
    // node *out = insert_sort(list);
    // node *out = insertionSortList(list);
    node *out = mergeSortList(list);
    node *temp = out;

    // pthread_mutex_lock(&lockBucket);
    all_buckets_t[strcut_data->thread_id] = (node *)malloc(sizeof(node));
    all_buckets_t[strcut_data->thread_id] = out;
    temp = all_buckets_t[strcut_data->thread_id];
    if(list != NULL)
    {
        // printf("this is the %d th bucket ",strcut_data->thread_id);
        while(temp!=NULL)
        {
            // printf("%d ",temp->data->key);
            temp = temp->next;
        }
        // printf("\n");
    }
    //exit thread
    // pthread_mutex_unlok(&lockBucket);
    pthread_exit(NULL);
}

//insert sort
node  *insert_sort(node *list)
{
    if(list == NULL || list->next == NULL)
    {
        return list;
    }
    
    node *k = list->next;
    node *nodeList = list;
    nodeList->next = NULL;
    
    while(k != NULL) 
    { 
        node *ptr;
        if(nodeList->data->key > k->data->key)  { 
            node *tmp;
            tmp = k;  
            k = k->next; 
            tmp->next = nodeList;
            nodeList = tmp; 
            continue;
        }

        for(ptr = nodeList; ptr->next != NULL; ptr = ptr->next) {
            if(ptr->next->data->key > k->data->key) break;
        }

        if(ptr->next!=NULL)
        {  
            node *tmp;
            tmp = k;  
            k = k->next; 
            tmp->next = ptr->next;
            ptr->next = tmp; 
        }
        else
        {
            ptr->next = k;  
            k = k->next;  
            ptr->next->next = NULL; 
        }
        // printf("k: %d\n", k->data->key);
    }
    return nodeList;
}

//my own insert sort
// node* insertionSortList(node* head) {
//         node* dummyHead = malloc(sizeof(node));
//         dummyHead->data = -2147483647;
//         dummyHead->next = head;
//         node* beforeHead = head;
//         head = head->next;
//         while (head != NULL){
// //            cout << head->val << endl;
//             node* start = dummyHead;
//             node* nextNode = head->next;
//             while(start != head && start != NULL) {
//                 if (head->data >= start->data && head->data <= start->next->data) {
//                     deleteNode(beforeHead);
//                     insertNode(start, head);
//                     break;
//                 }
//                 start = start->next;
//             }
//             if(start == beforeHead){
//                 beforeHead = beforeHead->next;
//             }
//             head = nextNode;
// //            beforeHead = beforeHead->next;
//         }
//         return dummyHead->next;
//     }

// void insertNode(node* root, node* insert){
//     node* next = root->next;
//     root->next = insert;
//     insert->next = next;
// }

// void deleteNode(node* before){
//     if(before->next == NULL) return;
//     before->next = before->next->next;
// }



    node* mergeSortList(node* head) {
        return sortList(head, NULL);
    }

    node* sortList(node* head, node* tail) {
        if (head == NULL) {
            return head;
        }
        if (head->next == tail) {
            head->next = NULL;
            return head;
        }
        node* slow = head, *fast = head;
        while (fast != tail) {
            slow = slow->next;
            fast = fast->next;
            if (fast != tail) {
                fast = fast->next;
            }
        }
        node* mid = slow;
        return merge(sortList(head, mid), sortList(mid, tail));
    }

    node* merge(node* head1, node* head2) {
        node* dummyHead = malloc(sizeof(node));
        dummyHead->data = 0;
        node* temp = dummyHead, *temp1 = head1, *temp2 = head2;
        while (temp1 != NULL && temp2 != NULL) {
            if (temp1->data->key <= temp2->data->key) {
                temp->next = temp1;
                temp1 = temp1->next;
            } else {
                temp->next = temp2;
                temp2 = temp2->next;
            }
            temp = temp->next;
        }
        if (temp1 != NULL) {
            temp->next = temp1;
        } else if (temp2 != NULL) {
            temp->next = temp2;
        }
        return dummyHead->next;
    }






int returnBucketIndex(int data)
{
    int ret;
    if(data < 0 )
        ret = data/RANGE_BUCKET * -1;
    else{
        ret = data/RANGE_BUCKET + MAX_BUCKET/2; 
    }
    return ret;
}


// void main()
// {
//     int i, size;

//     //initialization random seed
//     srand(time(0));
//     printf("before malloc\n");
//     int* array  = malloc(sizeof(int) * MAX_NUMBERS);
//     printf("after malloc\n");
//     for(i = 0; i<MAX_NUMBERS; i++)
//     {
//         array[i] = rand()%1000000;
//         printf("array %d : %d \n", i, array[i]);
//     }
//     printf("after array\n");

//     size = MAX_NUMBERS;
//     bucket_sort(array, size);
//     pthread_exit(NULL);
//     return;
// }


int main(int argc,char *argv[])
{
int i, size;
int fdin, fdout;
char *src, *dst;
struct stat statbuf;
int mode = 0x0777;
pthread_mutex_init(&lockBucket, NULL);


 if (argc != 3)
 {
fprintf(stderr,"An error has occurred\n");
return 0;
 }
   

     /* open the input file */
 if ((fdin = open (argv[1], O_RDONLY)) < 0)
   {  fprintf(stderr,"An error has occurred\n");
    
    return 0;
   }

 /* open/create the output file */
//  printf("argv2 %s \n", argv[2]);
fdout = open(argv[2], O_CREAT | O_TRUNC | O_RDWR,
           S_IRWXU | S_IRWXG | S_IRWXO );
 if (fdout <0)//edited here
   {fprintf(stderr,"An error has occurred\n");
    return 0;
   }
//    printf("line 271\n");
 /* find size of input file */
 if (fstat (fdin,&statbuf) < 0)
   {fprintf(stderr,"An error has occurred\n");
    return 0;
   }


   if(statbuf.st_size <=0){
    fprintf(stderr,"An error has occurred\n");
    return 0;
   }
 /* go to the location corresponding to the last byte */
//  if (lseek (fdout, statbuf.st_size - 1, SEEK_SET) == -1)
//    {printf ("lseek error");
//     return 0;
//    }
 /* write a dummy byte at the last location */
//  if (write (fdout, "1", 1) != 1)
//    {printf ("write error");
//      return 0;
//    }

   void* mapPtr;

 /* mmap the input file */
 if ((src = mmap (0, statbuf.st_size, PROT_READ, MAP_SHARED, fdin, 0))
   == (mapPtr) -1)
   {fprintf(stderr,"An error has occurred\n");
    return 0;
   }
 entryArray = (struct entry*)malloc(sizeof(struct entry)* statbuf.st_size/100);
//entryArray = malloc(sizeof(struct entry) * 100);
//    int *arr = malloc(10* sizeof(int));
//    arr[0] = 5;

//    struct entry *ptr;
//    ptr = (struct entry*)malloc(sizeof(struct entry) * 100);
//   ptr[i].key = *(src+i);
//     ptr[i].record = *(src+i+4);
// printf("sizeofInt : %ld\n", sizeof(int));
// printf("%ld\n",statbuf.st_size);
for(int i =0 ; i<statbuf.st_size/100;i++){
    // unsigned int* k = src + i * 100;
    // unsigned char* key = malloc(4);
    // memcpy(key, src + i *100, 4);
    // printf("k : %d\n", key[0]);
    // printf("k : %d\n", key[1]);
    // entryArray[i].key = byte_to_int(key);
    // entryArray[i].key = *k;
    entryArray[i].key =  *(int*)(src+i*100);
    entryArray[i].record = malloc(96);
    memcpy(entryArray[i].record, src+i*100+4, 96 );
    // printf("key: %d, record: %d\n",entryArray[i].key,entryArray[i].record[23]);
}

// printf("key : %d, divide by2 : %d", entryArray[99].key, entryArray[99].key/2);
int first = *((int*)(src));
// printf("first key: %d \n", first);
// printf("first record: %d\n",*(src+4));
//    for(int i=0;i<statbuf.st_size/100;i++){
//     printf("Key is : %d, value is %d",entryArray[i].key,entryArray[i].record[0]);
//    }



// printf("second element : %d\n", entryArray[1].key);


// printf("get output array, first element %d\n", outoutArray[0]);
// printf("get output array, second element %d\n", outoutArray[1]);
// printf("get output array, first element %d\n", outoutArray[25]);
// printf("get output array, second element %d\n", outoutArray[26]);


// int ftruncate(int fd, oflength) increase the size of the output file
ftruncate(fdout, statbuf.st_size);
 /* mmap the output file */
 if ((dst = mmap (0, statbuf.st_size, PROT_READ | PROT_WRITE,
   MAP_SHARED, fdout, 0)) == (mapPtr) -1)
   {fprintf(stderr,"An error has occurred\n");
    return 0;
   }

// printf("before memcpy\n");

int* outoutArray;
outoutArray = bucket_sort(entryArray, statbuf.st_size/100, dst);
// memcpy(dst, outoutArray, statbuf.st_size);

close(fdout);
close(fdin);
return 0;
}
