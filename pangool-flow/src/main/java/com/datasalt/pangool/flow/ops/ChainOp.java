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
package com.datasalt.pangool.flow.ops;

import java.io.IOException;

/**
 * A chain of {@link Op}s. This class is not thread-safe.
 */
@SuppressWarnings({ "serial", "unchecked", "rawtypes" })
public class ChainOp<T, K> extends Op<T, K> {

	Op[] ops;
	int methodCount = 0;
	ReturnCallback origCallback;

	public ChainOp(Op... ops) {
		this.ops = ops;
	}

	ReturnCallback callback = new ReturnCallback() {
		public void onReturn(Object element) throws IOException, InterruptedException {
			if(methodCount < ops.length) {
				ops[methodCount++].process(element, callback);
			} else {
				methodCount = 0;
				origCallback.onReturn(element);
			}
		};
	};

	@Override
	public void process(T input, ReturnCallback<K> callback) throws IOException, InterruptedException {
		origCallback = callback;
		ops[methodCount++].process(input, this.callback);
	}
}
