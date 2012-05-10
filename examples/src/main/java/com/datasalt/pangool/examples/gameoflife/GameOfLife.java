/**
 * Copyright [2012] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasalt.pangool.examples.gameoflife;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import com.datasalt.pangool.examples.gameoflife.GameOfLife.GameOfLifeException.CauseMessage;

/**
 * Simple class that can run a GOL simulation (http://en.wikipedia.org/wiki/The_Game_of_Life) using a fixed-size table
 * of {@link this.#MAX_X} x {@link this.#MAX_Y}. The simulation is run in a way that the table moves with the automata
 * (the automata is always centered in (0, 0)).
 * <p>
 * This class can be used to calculate how many iterations a certain configuration of GOL needs for convergence.
 */
public class GameOfLife {

	int width;
	long initialState;

	private int MAX_X = 16;
	private int MAX_Y = 16;
	private int MAX_ITERATIONS = 1000;

	byte[][] matrix;
	byte[][] auxMatrix;

	// Will keep track of all seen states to detect cycles
	Set<String> states = new HashSet<String>();

	public GameOfLife(int width, byte[] initialStateInBytes, int maxX, int maxY, int maxIterations)
	    throws GameOfLifeException {
		this.width = width;
		this.MAX_X = maxX;
		this.MAX_Y = maxY;
		this.MAX_ITERATIONS = maxIterations;
		matrix = new byte[maxX][maxY];
		auxMatrix = new byte[maxX][maxY];
		emptyMatrix(matrix, maxX, maxY);
		for(int i = 0; i < width; i++) {
			for(int j = 0; j < width; j++) {
				int offset = i * width + j;
				int nbyte = offset / 8;
				int byteOffset = offset % 8;
				if((initialStateInBytes[initialStateInBytes.length - 1 - nbyte] & (1 << byteOffset)) != 0) {
					matrix[i][j] = 1;
				}
			}
		}
		saveCurrentState();
	}

	// Create an empty matrix - we do this all around.
	// It is not very efficient but it is simple enough
	public static void emptyMatrix(byte[][] matrix, int maxX, int maxY) {
		for(int i = 0; i < maxX; i++) {
			for(int j = 0; j < maxY; j++) {
				matrix[i][j] = 0;
			}
		}
	}

	// This method is used for adjusting the current state to (0, 0) so that we can effectively identify repeated states
	protected static void adjustUpLeft(byte[][] matrix, byte[][] tempMatrix, int maxX, int maxY) {
		int currentMinX = Integer.MAX_VALUE, currentMinY = Integer.MAX_VALUE;
		for(int i = 0; i < maxX; i++) {
			for(int j = 0; j < maxY; j++) {
				if(matrix[i][j] == 1) {
					if(i < currentMinX) {
						currentMinX = i;
					}
					if(j < currentMinY) {
						currentMinY = j;
					}
				}
			}
		}
		for(int i = currentMinX; i < maxX; i++) {
			for(int j = currentMinY; j < maxY; j++) {
				tempMatrix[i - currentMinX][j - currentMinY] = matrix[i][j];
			}
		}
		for(int i = 0; i < maxX; i++) {
			for(int j = 0; j < maxY; j++) {
				matrix[i][j] = tempMatrix[i][j];
			}
		}
	}

	@SuppressWarnings("serial")
	public static class GameOfLifeException extends Exception {

		public static enum CauseMessage {
			GRID_OVERFLOW, MAX_ITERATIONS, CONVERGENCE_REACHED, NO_ALIVE_CELLS
		}

		CauseMessage cause;
		int iterations;

		public GameOfLifeException(CauseMessage cause, int iterations, String reason) {
			super(cause + ": " + reason);
			this.cause = cause;
			this.iterations = iterations;
		}

		public int getIterations() {
			return iterations;
		}

		public CauseMessage getCauseMessage() {
			return cause;
		}
	}

	/*
	 * Useful for debug, prints a matrix
	 */
	public static void printMatrix(byte[][] matrix, int maxX, int maxY, PrintStream stream) {
		for(int i = 0; i < maxX; i++) {
			for(int j = 0; j < maxY; j++) {
				stream.print(matrix[i][j] + " ");
			}
			stream.print("\n");
		}
		stream.println();
	}

	public void nextCycle() throws GameOfLifeException {
		// We initialize the next state as a x + 1, y + 1 of current state
		// We do this so next cells can be born around past state
		emptyMatrix(auxMatrix, MAX_X, MAX_Y);
		for(int i = 0; i < MAX_X; i++) {
			for(int j = 0; j < MAX_Y; j++) {
				if(matrix[i][j] == 1) {
					if(i == MAX_X - 1 || j == MAX_Y - 1) {
						throw new GameOfLifeException(CauseMessage.GRID_OVERFLOW, states.size(),
						    "Current state can't be evolved further with a " + MAX_X + "x" + MAX_Y + " grid.");
					}
					auxMatrix[i + 1][j + 1] = 1;
				}
			}
		}
		for(int i = 0; i < MAX_X; i++) {
			for(int j = 0; j < MAX_Y; j++) {
				matrix[i][j] = auxMatrix[i][j];
				auxMatrix[i][j] = 0;
			}
		}
		// Next we calculate whether some cells die or are born
		for(int i = 0; i < MAX_X; i++) {
			for(int j = 0; j < MAX_Y; j++) {
				int nNeighbors = 0;
				if(i > 0 && matrix[i - 1][j] == 1) {
					nNeighbors++;
				}
				if(i > 0 && j > 0 && matrix[i - 1][j - 1] == 1) {
					nNeighbors++;
				}
				if(i > 0 && j < MAX_Y - 1 && matrix[i - 1][j + 1] == 1) {
					nNeighbors++;
				}
				if(j > 0 && matrix[i][j - 1] == 1) {
					nNeighbors++;
				}
				if(j < MAX_Y - 1 && matrix[i][j + 1] == 1) {
					nNeighbors++;
				}
				if(i < MAX_X - 1 && j > 0 && matrix[i + 1][j - 1] == 1) {
					nNeighbors++;
				}
				if(i < MAX_X - 1 && matrix[i + 1][j] == 1) {
					nNeighbors++;
				}
				if(i < MAX_X - 1 && j < MAX_Y - 1 && matrix[i + 1][j + 1] == 1) {
					nNeighbors++;
				}
				if(matrix[i][j] == 1) {
					if(nNeighbors < 2 || nNeighbors > 3) {
						auxMatrix[i][j] = 0;
					} else {
						auxMatrix[i][j] = 1;
					}
				} else {
					if(nNeighbors == 3) {
						auxMatrix[i][j] = 1;
					} else {
						auxMatrix[i][j] = 0;
					}
				}
			}
		}
		// Substitute current state with next state
		int nAlives = 0;
		for(int i = 0; i < MAX_X; i++) {
			for(int j = 0; j < MAX_Y; j++) {
				matrix[i][j] = auxMatrix[i][j];
				if(matrix[i][j] == 1) {
					nAlives++;
				}
			}
		}
		if(nAlives == 0) {
			throw new GameOfLifeException(CauseMessage.NO_ALIVE_CELLS, states.size(), "No alive cells.");
		}
		// Adjust the current state up-left
		// This helps us identify repeated states
		emptyMatrix(auxMatrix, MAX_X, MAX_Y);
		adjustUpLeft(matrix, auxMatrix, MAX_X, MAX_Y);
		saveCurrentState();
	}

	protected void saveCurrentState() throws GameOfLifeException {
		StringBuffer buffer = new StringBuffer();
		for(int i = 0; i < MAX_X; i++) {
			for(int j = 0; j < MAX_Y; j++) {
				buffer.append((int) matrix[i][j]);
			}
		}
		String currentState = buffer.toString();
		if(states.contains(currentState)) {
			throw new GameOfLifeException(CauseMessage.CONVERGENCE_REACHED, states.size(), "Convergence reached after "
			    + states.size() + " states");
		} else {
			states.add(currentState);
			if(states.size() == MAX_ITERATIONS) {
				throw new GameOfLifeException(CauseMessage.MAX_ITERATIONS, states.size(), "Max iterations reached: "
				    + MAX_ITERATIONS);
			}
		}
	}

	protected byte[][] getMatrix() {
		return matrix;
	}

	// --------------------------------------------- //

	protected static long bytesToLong(byte[] bytes) {
		byte[] b = new byte[8];
		for(int i = 0; i < 8; i++) {
			b[i] = 0;
		}
		for(int i = 0; i < bytes.length; i++) {
			b[i] = bytes[i];
		}
		ByteBuffer buff = ByteBuffer.wrap(b);
		return buff.getLong();
	}

	protected static byte[] longToBytes(long initialState) {
		byte[] b = new byte[8];
		for(int i = 0; i < 8; i++) {
			b[7 - i] = (byte) (initialState >>> (i * 8));
		}
		return b;
	}
}