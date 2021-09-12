#include <iostream>
#include "Timer.h"
#include <stdlib.h>   // atoi
#include <limits.h>
#include "mpi.h"
#include <omp.h>

int default_size = 100;  // the default system size
int defaultCellWidth = 8;
double c = 1.0;      // wave speed
double dt = 0.1;     // time quantum
double dd = 2.0;     // change in system

using namespace std;

int main( int argc, char *argv[] ) {
	
  int rank;
  int mpi_size;
  
  // verify arguments
  if ( argc != 5 ) {
    cerr << "usage: Wave2D size max_time interval" << endl;
    return -1;
  }
  int size = atoi( argv[1] );
  int max_time = atoi( argv[2] );
  int interval  = atoi( argv[3] );
  int nthreads = atoi(argv[4]); 
  if(interval == 0){
	interval = INT_MAX;
  }

  if ( size < 100 || max_time < 3 || interval < 0 ) {
    cerr << "usage: Wave2D size max_time interval" << endl;
    cerr << "       where size >= 100 && time >= 3 && interval >= 0" << endl;
    return -1;
  }
  double z[3][size][size];

  
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
  
  omp_set_num_threads(nthreads);
  int stripe = size / mpi_size;
  int start_index; 
  int end_index;
  int diffusionTime = 2; 
  // divide the start and end index between ranks
  if(rank == 0){
	start_index = stripe * rank;
	end_index = start_index + stripe +(size % mpi_size)-1;
  }
  else{
	start_index = stripe * rank + (size % mpi_size);
	end_index = start_index + stripe -1;
  }
  
  cerr << "rank[" << rank << "]'s range = " << start_index << "~" << end_index << endl;
  
  
  // create a simulation space
  
  for ( int p = 0; p < 3; p++ ) 
    for ( int i = 0; i < size; i++ )
      for ( int j = 0; j < size; j++ )
	z[p][i][j] = 0.0; // no wave

  diffusionTime++;
  // start a timer
  Timer time;
  time.start( );
    int weight = size / default_size;

  // time = 0;
  // initialize the simulation space: calculate z[0][][]
  // parallelize with multithreaded
  #pragma omp parallel for firstprivate(size, weight) shared(z)
 
  for( int i = 0; i < size; i++ ) {
    for( int j = 0; j < size; j++ ) {
      if( i > 40 * weight && i < 60 * weight  &&
	  j > 40 * weight && j < 60 * weight ) {
	z[0][i][j] = 20.0;
      } else {
	z[0][i][j] = 0.0;
      }
    }
  }
  diffusionTime++;

  // time = 1
  // calculate z[1][][] 
  // cells not on edge
  // parallelize with multithreaded
  #pragma omp parallel for firstprivate(size) shared(z)
  for (int i = 0; i < size; i++){
		for(int j =0; j < size; j++){
			if(i == 0 || i == (size -1) || j == 0 || j == (size -1)){
				z[1][i][j] = 0.0;
			}
			else{
				z[1][i][j] = z[0][i][j] + (c * c) / 2 * ((dt / dd)*(dt / dd))*
				(z[0][i + 1][j] + z[0][i - 1][j] + z[0][i][j + 1] + z[0][i][j - 1] - 4.0 * z[0][i][j]);
			}
			
		}
   }

    diffusionTime++;
  
  // simulate wave diffusion from time = 2
  for ( int t = 2; t < max_time; t++ ) {
    
    // parallelize the most computational part with multithreaded
    #pragma omp parallel for firstprivate(start_index, end_index, t) shared(z)
	for (int i = start_index; i <= end_index; i++){
		for(int j =0; j < size; j++){
			if(i == 0 || i == size -1 || j == 0 || j == size -1){
				z[(t % 3)][i][j] = 0.0;
			}else{	
			    z[(t % 3)][i][j] = 2.0 * z[(t - 1) % 3][i][j] - z[(t - 2) % 3][i][j] + (c*c) * (dt / dd)* (dt / dd)*
				(z[(t - 1) % 3][i + 1][j] + z[(t - 1) % 3][i - 1][j] + z[(t - 1) % 3][i][j - 1] + z[(t - 1) % 3][i][j + 1] - 
				4.0 * z[(t - 1) % 3][i][j]);
					
			}
			
		}
	}
    // rank 0, 1, 2 send data to right neighbour
    if(rank < mpi_size-1){
         MPI_Send(&z[t % 3][end_index], size, MPI_DOUBLE, rank + 1, 0, MPI_COMM_WORLD);   
    }
	// rank 1,2,3 send data to left neighbour
	if(rank > 0){
		MPI_Send(&z[t % 3][start_index], size, MPI_DOUBLE, rank - 1, 0, MPI_COMM_WORLD);
	}
    // rank 0, 1, 2 receive data from right neighbour
    if(rank < mpi_size -1){
       	MPI_Recv(&z[t % 3][end_index + 1], size, MPI_DOUBLE, rank + 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
	// rank 1,2,3 receive data from left neighbours
	if(rank > 0){
		MPI_Recv(&z[t % 3][start_index - 1], size, MPI_DOUBLE, rank - 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
	}
	
	// master printsout result when diffusion time is divisible by interval  
	if(diffusionTime % interval == 0){
        // rank 1, 2,3 send data to master
		if(rank > 0){
			for(int i = start_index; i <= end_index; i++){
				 MPI_Send(&z[t % 3][i], size, MPI_DOUBLE, 0, 30, MPI_COMM_WORLD);
            }
		}
		// master receive data from all slaves
		else{
			for(int my_rank = 1; my_rank < mpi_size; my_rank++){           
				int start = stripe * my_rank + (size % mpi_size);
			    int end = start + stripe -1;
				for(int i = start; i <= end; i++){
					 MPI_Recv(&z[t % 3][i], size, MPI_DOUBLE, my_rank, 30, MPI_COMM_WORLD, MPI_STATUS_IGNORE);                       		
	            }
			}
		}
        // master process prints out data
		if(rank == 0){
			cout << diffusionTime << endl;
		    for(int i = 0; i < size; i++){
			    for (int j = 0; j < size; j++){
				    cout << z[t%3][i][j] << " ";
			}
            cout << "\n";
		}
        cout << "\n";
		}           
	}
	diffusionTime++;

  } // end of simulation
  
    
  // finish the timer
  if(rank == 0){
	cerr << "Elapsed time = " << time.lap( ) << endl;
  }

  MPI_Finalize();

  return 0;
}


