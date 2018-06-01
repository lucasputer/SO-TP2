#include "node.h"
#include "picosha2.h"
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <cstdlib>
#include <queue>
#include <atomic>
#include <mpi.h>
#include <map>
#include <chrono>
#include <thread>

int total_nodes, mpi_rank;
Block *last_block_in_chain;
map<string,Block> node_blocks;
<<<<<<< HEAD
pthread_mutex_t broadcast_mutex;
pthread_mutex_t migrate_chain_semaphore;
bool adding_new_block = false;
bool building_chain = false;
bool waiting_for_chain = true;
=======
pthread_mutex_t general_mutex;
pthread_mutex_t migrate_chain_mutex;
int finished_nodes = 0;
int i_have_finished = false;

>>>>>>> fff72f4e4409df1f29ab3a82fbc29c7cd863d4be
Block blockchain[VALIDATION_BLOCKS];

void validate_block(Block* block, bool* hash_validation, bool* node_blocks_validation){
    string hash_hex_str;
    block_to_hash(block,hash_hex_str);

    if(hash_hex_str != block->block_hash){
        *hash_validation = false;
    }

    if(!*node_blocks_validation){
        *node_blocks_validation = node_blocks.count(string(block->block_hash));
    }
}

//Envia el bloque minado a todos los nodos
void broadcast_block(const Block *block){
    MPI_Request m;
    for(int i = mpi_rank + 1; i < total_nodes; i++){
        MPI_Isend(block, 1, *MPI_BLOCK, i, TAG_NEW_BLOCK, MPI_COMM_WORLD,&m);
    }
    for(int i = 0; i < mpi_rank; i++){
        MPI_Isend(block, 1, *MPI_BLOCK, i, TAG_NEW_BLOCK, MPI_COMM_WORLD,&m);
    }
    printf("[%d] Broadcast de mi producido con index %d \n", mpi_rank, block->index);
}

void broadcast_finished(){
    MPI_Request m;
    i_have_finished = true;
    finished_nodes++;
    int j =0;
    for(int i = mpi_rank + 1; i < total_nodes; i++){
        MPI_Isend(&j, 1, MPI_INT, i, TAG_FINISHED, MPI_COMM_WORLD,&m);
    }
    for(int i = 0; i < mpi_rank; i++){
        MPI_Isend(&j, 1, MPI_INT, i, TAG_FINISHED, MPI_COMM_WORLD,&m);
    }
    printf("[%d] Broadcast de que termine\n", mpi_rank);
}

//Cuando me llega una cadena adelantada, y tengo que pedir los nodos que me faltan
//Si nos separan más de VALIDATION_BLOCKS bloques de distancia entre las cadenas, se descarta por seguridad
bool verificar_y_migrar_cadena(const Block *rBlock, const MPI_Status *status){
    pthread_mutex_lock(&migrate_chain_mutex);
    printf("[%d] Pidiendo cadena a %d con index %d \n",mpi_rank,status->MPI_SOURCE, rBlock->index);
    MPI_Send(rBlock, 1, *MPI_BLOCK, status->MPI_SOURCE, TAG_CHAIN_HASH, MPI_COMM_WORLD);
    printf("[%d] Pedí cadena a %d con index %d \n",mpi_rank,status->MPI_SOURCE, rBlock->index);
    pthread_mutex_lock(&migrate_chain_mutex);
    pthread_mutex_unlock(&migrate_chain_mutex);

    printf("[%d] Recibí la cadena enviada por %d \n",mpi_rank,status->MPI_SOURCE);
    //TODO: Verificar que los bloques recibidos
    //sean válidos y se puedan acoplar a la cadena
    printf("[%d] hash1 %s , hash2 %s \n",mpi_rank,blockchain[0].block_hash,rBlock->block_hash);
    printf("[%d] index1 %d , index2 %d \n",mpi_rank,blockchain[0].index,rBlock->index);
    //El primer bloque de la lista contiene el hash pedido y el mismo index que el bloque original.
    string hash1 = rBlock->block_hash;
    string hash2 = blockchain[0].block_hash;
    if(hash1.compare(hash2) != 0 || rBlock->index != blockchain[0].index){
       printf("[%d] Descarto (primer cond) cadena enviada por %d \n", mpi_rank,status->MPI_SOURCE);
        return false;
    }

    //El hash del bloque recibido es igual al calculado por la función block_to_hash.
    //Cada bloque siguiente de la lista, contiene el hash definido en previous_block_hash del actual elemento.
    //Cada bloque siguiente de la lista, contiene el índice anterior al actual elemento.
    //algun bloque estaba en el diccionario
    int i = 0;
    //creo diff para saber la cantidad de nodos que tengo que recibir
    //si diff es mas que validation blocks no se que onda.
    int diff = blockchain[0].index - last_block_in_chain->index;

    Block current = blockchain[i];
    Block prev = blockchain[i];
    bool validated = true;
    bool any_in_node_blocks = false;

    while(i < diff && validated){
        validate_block(&current, &validated, &any_in_node_blocks);
        if((i > 0) && (current.block_hash != prev.previous_block_hash || current.index + 1 != prev.index))
            validated = false;
        
        i++;
        prev = current;
        current = current = blockchain[i];
    }

    if(validated && any_in_node_blocks){
        //migrar
        i = 0;
        current = blockchain[i];
        while(i < diff){
            node_blocks[string(current.block_hash)] = current;
            i++;
            current = blockchain[i];
        }

        *last_block_in_chain =  blockchain[0];
        if(blockchain[0].index == MAX_BLOCKS)
            broadcast_finished();

        printf("[%d] Acepté cadena enviada por %d mi nuevo index es %d\n", mpi_rank,status->MPI_SOURCE, last_block_in_chain->index);

        return true;
    }
    printf("[%d] Descarto (segunda cond cond) cadena enviada por %d \n", mpi_rank,status->MPI_SOURCE);

    return false;
}

//Verifica que el bloque tenga que ser incluido en la cadena, y lo agrega si corresponde
bool validate_block_for_chain(const Block *rBlock, const MPI_Status *status){
    if(valid_new_block(rBlock)){
        //Agrego el bloque al diccionario, aunque no
        //necesariamente eso lo agrega a la cadena
        node_blocks[string(rBlock->block_hash)]=*rBlock;

        //Si el índice del bloque recibido es 1
        //y mí último bloque actual tiene índice 0,
        //entonces lo agrego como nuevo último.
        if(rBlock->index == 1 && last_block_in_chain->index == 0){
            *last_block_in_chain = *rBlock;
            printf("[%d] Agregado a la lista bloque con index %d enviado por %d \n", mpi_rank, rBlock->index,status->MPI_SOURCE);
            if(rBlock->index == MAX_BLOCKS)
                    broadcast_finished();
            return true;
        }else if(last_block_in_chain->index + 1 == rBlock->index ){
            //chequear si esto esta bien o hay que hacerlo con block_to_hash
            string hash1 = rBlock->previous_block_hash;
            string hash2 = last_block_in_chain->block_hash;
            if(hash1.compare(hash2) == 0){
                //Si el índice del bloque recibido es
                //el siguiente a mí último bloque actual,
                //y el bloque anterior apuntado por el recibido es mí último actual,
                //entonces lo agrego como nuevo último.
                printf("[%d] Agregado a la lista bloque con index %d enviado por %d \n", mpi_rank, last_block_in_chain->index,status->MPI_SOURCE);
                *last_block_in_chain = *rBlock;
                if(rBlock->index == MAX_BLOCKS)
                    broadcast_finished();
                return true;
            }else{
                //Si el índice del bloque recibido es
                //el siguiente a mí último bloque actual,
                //pero el bloque anterior apuntado por el recibido no es mí último actual,
                //entonces hay una blockchain más larga que la mía.
                printf("[%d] Perdí la carrera por uno (%d) contra %d \n", mpi_rank, rBlock->index, status->MPI_SOURCE);
                bool res = verificar_y_migrar_cadena(rBlock,status);
                return res;
            }
        }else if(last_block_in_chain->index >= rBlock->index){
            //-Si el índice del bloque recibido es igua al índice de mi último bloque actual,
            //entonces hay dos posibles forks de la blockchain pero mantengo la mía
            //-Si el índice del bloque recibido es anterior al índice de mi último bloque actual,
            //entonces lo descarto porque asumo que mi cadena es la que está quedando preservada.
            printf("[%d] Conflicto suave: Conflicto de branch (%d) contra %d \n",mpi_rank,rBlock->index,status->MPI_SOURCE);
            return false;    
        }else if(last_block_in_chain->index + 1 < rBlock->index){
            //Si el índice del bloque recibido está más de una posición adelantada a mi último bloque actual,
            //entonces me conviene abandonar mi blockchain actual
            printf("[%d] Perdí la carrera por varios contra %d \n", mpi_rank, status->MPI_SOURCE);
            bool res = verificar_y_migrar_cadena(rBlock,status);
            return res;
        }
    }
    printf("[%d] Error duro: Descarto el bloque recibido de %d porque no es válido \n",mpi_rank,status->MPI_SOURCE);
    return false;
}

//Proof of work
//TODO: Advertencia: puede tener condiciones de carrera
void* proof_of_work(void *ptr){
    string hash_hex_str;
    Block block;
    unsigned int mined_blocks = 0;
    while(!i_have_finished){
        block = *last_block_in_chain;

        //Preparar nuevo bloque
        block.index += 1;
        block.node_owner_number = mpi_rank;
        block.difficulty = DEFAULT_DIFFICULTY;
        block.created_at = static_cast<unsigned long int> (time(NULL));
        memcpy(block.previous_block_hash,block.block_hash,HASH_SIZE);

        //Agregar un nonce al azar al bloque para intentar resolver el problema
        gen_random_nonce(block.nonce);

        //Hashear el contenido (con el nuevo nonce)
        block_to_hash(&block,hash_hex_str);

        //Contar la cantidad de ceros iniciales (con el nuevo nonce)
        if(solves_problem(hash_hex_str)){
            //Verifico que no haya cambiado mientras calculaba
<<<<<<< HEAD
            
            if(!adding_new_block && last_block_in_chain->index < block.index){
=======
            pthread_mutex_lock(&general_mutex);
            if(last_block_in_chain->index < block.index){
>>>>>>> fff72f4e4409df1f29ab3a82fbc29c7cd863d4be
                
                mined_blocks += 1;
                *last_block_in_chain = block;
                strcpy(last_block_in_chain->block_hash, hash_hex_str.c_str());
                last_block_in_chain->created_at = static_cast<unsigned long int> (time(NULL));
                node_blocks[hash_hex_str] = *last_block_in_chain;
                printf("[%d] Agregué un bloque producido con index %d \n",mpi_rank,last_block_in_chain->index);
                //Mientras comunico, no responder mensajes de nuevos nodos
                if(block.index == MAX_BLOCKS){
                    broadcast_finished();
                }

                broadcast_block(last_block_in_chain);
                pthread_mutex_unlock(&general_mutex);
            }
            
        }

    }
    printf("[%d] Termine de minar \n", mpi_rank);
    return NULL;
}

void* build_chain_response(void* ptr) {

    ChainBuildData * data = (ChainBuildData*)(ptr);
    Block blockchain_local[VALIDATION_BLOCKS];
    building_chain = true;
    //Construimos la cadena
    printf("[%d] Construyendo la cadena pedida por %d\n",mpi_rank,data->mpi_source);
    string block_hash =  data->block_hash;

    blockchain_local[0] = node_blocks.find(block_hash)->second;
    int i = 1;
    Block current_block;
    while(i < VALIDATION_BLOCKS){
        current_block = node_blocks.find(block_hash)->second;
        blockchain_local[i] = current_block;
        block_hash = current_block.previous_block_hash;
        i++; 
    }
    printf("[%d] Envío la cadena pedida por %d con tamano %d\n",mpi_rank,data->mpi_source,i);
    MPI_Send(&blockchain_local, i, *MPI_BLOCK, data->mpi_source, TAG_CHAIN_RESPONSE, MPI_COMM_WORLD);
    building_chain = false;
    return 0;
}

void * add_new_block(void *ptr) {
    //hay un unico thread ejecutando esta funcion. ASi que el booleano no hace falta que sea atomico
    NewBlockData* data = (NewBlockData*)(ptr);
    pthread_mutex_lock(&general_mutex);
    validate_block_for_chain(&(data->new_block), &(data->status));
    pthread_mutex_unlock(&general_mutex);
    return 0;

}

int node(){

    //Tomar valor de mpi_rank y de nodos totales
    MPI_Comm_size(MPI_COMM_WORLD, &total_nodes);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

    //La semilla de las funciones aleatorias depende del mpi_ranking
    srand(time(NULL) + mpi_rank);
    printf("[MPI] Lanzando proceso %u\n", mpi_rank);

    last_block_in_chain = new Block;

    //Inicializo el primer bloque
    last_block_in_chain->index = 0;
    last_block_in_chain->node_owner_number = mpi_rank;
    last_block_in_chain->difficulty = DEFAULT_DIFFICULTY;
    
    memset(last_block_in_chain->previous_block_hash,0,HASH_SIZE);

    //Inicializo el broadcast mutex
    pthread_mutex_init(&general_mutex,NULL);
    pthread_mutex_init(&migrate_chain_mutex,NULL);
    //Crear thread para minar
    pthread_t mining;
    pthread_create(&mining,NULL,proof_of_work,NULL);
    
    while(finished_nodes < total_nodes){
        MPI_Status status;
        MPI_Probe(MPI_ANY_SOURCE,MPI_ANY_TAG,MPI_COMM_WORLD,&status);
        //TODO: Recibir mensajes de otros nodos
        if(status.MPI_TAG == TAG_NEW_BLOCK){
            Block block; 
            MPI_Recv(&block,1,*MPI_BLOCK,status.MPI_SOURCE,TAG_NEW_BLOCK,MPI_COMM_WORLD,&status);
            printf("--- [%d] Recibí de %d un bloque con index %d \n",mpi_rank,status.MPI_SOURCE,block.index);
            pthread_t add_new_block_thread;
            NewBlockData * data = new NewBlockData;
            data->new_block = block;
            data->status = status;
            pthread_create(&add_new_block_thread, NULL, add_new_block, (void*)(data));
            //no hago join. Xq no quiero bloquear el loop. Ya que quiero seguir escuchando mensajes
        }else if(!building_chain && status.MPI_TAG ==TAG_CHAIN_HASH){
            Block requested_block_hash;
            MPI_Recv(&requested_block_hash,1,*MPI_BLOCK,status.MPI_SOURCE,TAG_CHAIN_HASH,MPI_COMM_WORLD, &status);
            printf("--- [%d] Recibí pedido de cadena de %d con index %d \n",mpi_rank,status.MPI_SOURCE,requested_block_hash.index);

            ChainBuildData * data = new ChainBuildData;
            data->block_hash = requested_block_hash.block_hash;
            data->mpi_source = status.MPI_SOURCE;
            pthread_t build_chain;
            pthread_create(&build_chain,NULL,build_chain_response,(void*)(data));
            //no hago join. Xq no quiero bloquear el loop. Ya que quiero seguir escuchando mensajes
        }else if(status.MPI_TAG ==TAG_CHAIN_RESPONSE){
            //copiar de la respuesta la blockchain a la variable global blockchain y 
            //hacer un unlock del mutex para el thread que esta añadiendo el bloque    
            //printf("[%d] Recibí cadena pedida de %d \n",mpi_rank,status.MPI_SOURCE);
            MPI_Status recv_status;
            MPI_Recv(blockchain,VALIDATION_BLOCKS,*MPI_BLOCK,status.MPI_SOURCE,TAG_CHAIN_RESPONSE,MPI_COMM_WORLD, &recv_status);
            printf("--- [%d] Desbloqueo thread agregando nodo  ... de %d \n",mpi_rank,status.MPI_SOURCE);
            pthread_mutex_unlock(&migrate_chain_mutex);
            printf("--- [%d] Copiando cadena de %d \n",mpi_rank,status.MPI_SOURCE);
        }else if(status.MPI_TAG == TAG_FINISHED){
            pthread_mutex_lock(&general_mutex);
            finished_nodes++;
            pthread_mutex_unlock(&general_mutex);
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);
    printf("[%d] Termine mi ejecucion \n", mpi_rank);
    delete last_block_in_chain;
    return 0;
}

