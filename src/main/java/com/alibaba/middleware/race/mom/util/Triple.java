/**
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *  
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 */
package com.alibaba.middleware.race.mom.util;

public class Triple<L, M, R> {

	private final L left;
	private final M middle;
	private final R right;

	public Triple(L left, M middle, R right) {
		if (left == null) {
			throw new IllegalArgumentException("Left value is not effective.");
		}
		if (middle == null) {
			throw new IllegalArgumentException("Middle value is not effective.");
		}
		if (right == null) {
			throw new IllegalArgumentException("Right value is not effective.");
		}
		this.left = left;
		this.middle = middle;
		this.right = right;
	}

	public L getLeft() {
		return this.left;
	}

	public M getMiddle() {
		return this.middle;
	}

	public R getRight() {
		return this.right;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((left == null) ? 0 : left.hashCode());
		result = prime * result + ((middle == null) ? 0 : middle.hashCode());
		result = prime * result + ((right == null) ? 0 : right.hashCode());
		return result;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Triple<Object, Object, Object> other = (Triple<Object, Object, Object>) obj;
		if (left == null) {
			if (other.left != null)
				return false;
		} else if (!left.equals(other.left))
			return false;
		if (middle == null) {
			if (other.middle != null)
				return false;
		} else if (!middle.equals(other.middle))
			return false;
		if (right == null) {
			if (other.right != null)
				return false;
		} else if (!right.equals(other.right))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "<" + left + "," + middle + "," + right + ">";
	}

}
