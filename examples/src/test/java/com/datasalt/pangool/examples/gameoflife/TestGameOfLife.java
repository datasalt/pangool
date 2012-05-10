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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.datasalt.pangool.examples.gameoflife.GameOfLife.GameOfLifeException;

public class TestGameOfLife {

	@Test
	public void testLongToBytes() {
		byte[] bytes1 = new byte[] { 0, 0, 1, 8, 0, 0, 7, 16 };
		long val = GameOfLife.bytesToLong(bytes1);
		byte[] bytes2 = GameOfLife.longToBytes(val);
		assertEquals(bytes2[0], 0);
		assertEquals(bytes2[1], 0);
		assertEquals(bytes2[2], 1);
		assertEquals(bytes2[3], 8);
		assertEquals(bytes2[4], 0);
		assertEquals(bytes2[5], 0);
		assertEquals(bytes2[6], 7);
		assertEquals(bytes2[7], 16);
	}

	@Test
	public void testAdjustUpLeft1() {
		byte[][] matrix = new byte[4][4];
		byte[][] auxMatrix = new byte[4][4];
		GameOfLife.emptyMatrix(matrix, 4, 4);
		GameOfLife.emptyMatrix(auxMatrix, 4, 4);
		matrix[1][1] = 1;
		matrix[2][2] = 1;
		GameOfLife.adjustUpLeft(matrix, auxMatrix, 4, 4);
		assertEquals(matrix[0][0], 1);
		assertEquals(matrix[1][1], 1);

		assertEquals(matrix[2][2], 0);
	}

	@Test
	public void testNextCycle() throws GameOfLifeException {
		byte[] bytes = new byte[] { -52, 51 };
		GameOfLife gameOfLife = new GameOfLife(4, bytes, 8, 8, 10);
		assertEquals(1, gameOfLife.getMatrix()[0][0]);
		assertEquals(1, gameOfLife.getMatrix()[0][1]);
		assertEquals(0, gameOfLife.getMatrix()[0][2]);
		assertEquals(0, gameOfLife.getMatrix()[0][3]);

		assertEquals(1, gameOfLife.getMatrix()[1][0]);
		assertEquals(1, gameOfLife.getMatrix()[1][1]);
		assertEquals(0, gameOfLife.getMatrix()[1][2]);
		assertEquals(0, gameOfLife.getMatrix()[1][3]);

		assertEquals(0, gameOfLife.getMatrix()[2][0]);
		assertEquals(0, gameOfLife.getMatrix()[2][1]);
		assertEquals(1, gameOfLife.getMatrix()[2][2]);
		assertEquals(1, gameOfLife.getMatrix()[2][3]);

		assertEquals(0, gameOfLife.getMatrix()[3][0]);
		assertEquals(0, gameOfLife.getMatrix()[3][1]);
		assertEquals(1, gameOfLife.getMatrix()[3][2]);
		assertEquals(1, gameOfLife.getMatrix()[3][3]);

		gameOfLife.nextCycle();

		assertEquals(1, gameOfLife.getMatrix()[0][0]);
		assertEquals(1, gameOfLife.getMatrix()[0][1]);
		assertEquals(0, gameOfLife.getMatrix()[0][2]);
		assertEquals(0, gameOfLife.getMatrix()[0][3]);

		assertEquals(1, gameOfLife.getMatrix()[1][0]);
		// Cell died
		assertEquals(0, gameOfLife.getMatrix()[1][1]);
		assertEquals(0, gameOfLife.getMatrix()[1][2]);
		assertEquals(0, gameOfLife.getMatrix()[1][3]);

		assertEquals(0, gameOfLife.getMatrix()[2][0]);
		assertEquals(0, gameOfLife.getMatrix()[2][1]);
		// Cell died
		assertEquals(0, gameOfLife.getMatrix()[2][2]);
		assertEquals(1, gameOfLife.getMatrix()[2][3]);

		assertEquals(0, gameOfLife.getMatrix()[3][0]);
		assertEquals(0, gameOfLife.getMatrix()[3][1]);
		assertEquals(1, gameOfLife.getMatrix()[3][2]);
		assertEquals(1, gameOfLife.getMatrix()[3][3]);

		try {
			gameOfLife.nextCycle();
		} catch(GameOfLifeException e) {
			assertTrue(e.getMessage().toLowerCase().contains("convergence reached"));
		}

		assertEquals(1, gameOfLife.getMatrix()[0][0]);
		assertEquals(1, gameOfLife.getMatrix()[0][1]);
		assertEquals(0, gameOfLife.getMatrix()[0][2]);
		assertEquals(0, gameOfLife.getMatrix()[0][3]);

		assertEquals(1, gameOfLife.getMatrix()[1][0]);
		// Cell is born
		assertEquals(1, gameOfLife.getMatrix()[1][1]);
		assertEquals(0, gameOfLife.getMatrix()[1][2]);
		assertEquals(0, gameOfLife.getMatrix()[1][3]);

		assertEquals(0, gameOfLife.getMatrix()[2][0]);
		assertEquals(0, gameOfLife.getMatrix()[2][1]);
		// Cell is born
		assertEquals(1, gameOfLife.getMatrix()[2][2]);
		assertEquals(1, gameOfLife.getMatrix()[2][3]);

		assertEquals(0, gameOfLife.getMatrix()[3][0]);
		assertEquals(0, gameOfLife.getMatrix()[3][1]);
		assertEquals(1, gameOfLife.getMatrix()[3][2]);
		assertEquals(1, gameOfLife.getMatrix()[3][3]);
	}

	@Test
	public void testAdjustUpLeft2() {
		byte[][] matrix = new byte[4][4];
		byte[][] auxMatrix = new byte[4][4];
		GameOfLife.emptyMatrix(matrix, 4, 4);
		GameOfLife.emptyMatrix(auxMatrix, 4, 4);
		matrix[1][3] = 1;
		matrix[2][2] = 1;
		GameOfLife.adjustUpLeft(matrix, auxMatrix, 4, 4);
		assertEquals(matrix[0][1], 1);
		assertEquals(matrix[1][0], 1);

		assertEquals(matrix[1][3], 0);
		assertEquals(matrix[2][2], 0);
	}

	@Test
	public void testSetBigInitialState() throws GameOfLifeException {
		byte[] bigState = new byte[] { 1, 0, 20, 20 };
		GameOfLife gameOfLife = new GameOfLife(5, bigState, 8, 8, 10);
		assertEquals(0, gameOfLife.getMatrix()[0][0]);
		assertEquals(0, gameOfLife.getMatrix()[0][1]);
		assertEquals(1, gameOfLife.getMatrix()[0][2]);
		assertEquals(0, gameOfLife.getMatrix()[0][3]);
		assertEquals(1, gameOfLife.getMatrix()[0][4]);

		assertEquals(0, gameOfLife.getMatrix()[1][0]);
		assertEquals(0, gameOfLife.getMatrix()[1][1]);
		assertEquals(0, gameOfLife.getMatrix()[1][2]);
		assertEquals(0, gameOfLife.getMatrix()[1][3]);
		assertEquals(0, gameOfLife.getMatrix()[1][4]);

		assertEquals(1, gameOfLife.getMatrix()[2][0]);
		assertEquals(0, gameOfLife.getMatrix()[2][1]);
		assertEquals(1, gameOfLife.getMatrix()[2][2]);
		assertEquals(0, gameOfLife.getMatrix()[2][3]);
		assertEquals(0, gameOfLife.getMatrix()[2][4]);

		assertEquals(0, gameOfLife.getMatrix()[3][0]);
		assertEquals(0, gameOfLife.getMatrix()[3][1]);
		assertEquals(0, gameOfLife.getMatrix()[3][2]);
		assertEquals(0, gameOfLife.getMatrix()[3][3]);
		assertEquals(0, gameOfLife.getMatrix()[3][4]);

		assertEquals(0, gameOfLife.getMatrix()[4][0]);
		assertEquals(0, gameOfLife.getMatrix()[4][1]);
		assertEquals(0, gameOfLife.getMatrix()[4][2]);
		assertEquals(0, gameOfLife.getMatrix()[4][3]);
		assertEquals(1, gameOfLife.getMatrix()[4][4]);
	}

	@Test
	public void testSetInitialState() throws GameOfLifeException {
		byte[] smallState = new byte[] { 11 }; // 00001011 <-
		// Interpreted as a grid of 2x2
		GameOfLife gameOfLife = new GameOfLife(2, smallState, 8, 8, 10);
		assertEquals(1, gameOfLife.getMatrix()[0][0]);
		assertEquals(1, gameOfLife.getMatrix()[0][1]);
		assertEquals(0, gameOfLife.getMatrix()[1][0]);
		assertEquals(1, gameOfLife.getMatrix()[1][1]);

		// Interpreted as a grid of 4x4
		smallState = new byte[] { 0, 11 };
		gameOfLife = new GameOfLife(4, smallState, 8, 8, 10);
		assertEquals(1, gameOfLife.getMatrix()[0][0]);
		assertEquals(1, gameOfLife.getMatrix()[0][1]);
		assertEquals(0, gameOfLife.getMatrix()[0][2]);
		assertEquals(1, gameOfLife.getMatrix()[0][3]);
		for(int i = 1; i < 4; i++) {
			for(int j = 0; j < 4; j++) {
				assertEquals(0, gameOfLife.getMatrix()[i][j]);
			}
		}
	}
}
