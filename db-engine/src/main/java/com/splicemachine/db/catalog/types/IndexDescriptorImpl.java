/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2018 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.catalog.types;

import com.splicemachine.db.catalog.IndexDescriptor;
import com.splicemachine.db.iapi.services.io.Formatable;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.IOException;

/**
 *
 *	This class implements Formatable. That means that it
 *	can write itself to and from a formatted stream. If
 *	you add more fields to this class, make sure that you
 *	also write/read them with the writeExternal()/readExternal()
 *	methods.
 *
 *	If, inbetween releases, you add more fields to this class,
 *	then you should bump the version number emitted by the getTypeFormatId()
 *	method.
 *
 * @see com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator
 *
 */
public class IndexDescriptorImpl implements IndexDescriptor, Formatable {
	private boolean		isUnique;
	private int[]		baseColumnPositions;
	private boolean[]	isAscending;
	private int			numberOfOrderedColumns;
	private String		indexType;
	//attribute to indicate the indicates allows duplicate only in
	//case of non null keys. This attribute has no effect if the isUnique
    //is true. If isUnique is false and isUniqueWithDuplicateNulls is set 
    //to true the index will allow duplicate nulls but for non null keys 
    //will act like a unique index.
	private boolean     isUniqueWithDuplicateNulls;
	private boolean 	excludeNulls;
	private boolean 	excludeDefaults;



	/**
     * Constructor for an IndexDescriptorImpl
     * 
     * @param indexType		The type of index
     * @param isUnique		True means the index is unique
     * @param isUniqueWithDuplicateNulls True means the index will be unique
     *                              for non null values but duplicate nulls
     *                              will be allowed.
     *                              This parameter has no effect if the isUnique
     *                              is true. If isUnique is false and 
     *                              isUniqueWithDuplicateNulls is set to true the
     *                              index will allow duplicate nulls but for
     *                              non null keys will act like a unique index.
     * @param baseColumnPositions	An array of column positions in the base
     * 								table.  Each index column corresponds to a
     * 								column position in the base table.
     * @param isAscending	An array of booleans telling asc/desc on each
     * 						column.
     * @param numberOfOrderedColumns	In the future, it will be possible
     * 									to store non-ordered columns in an
     * 									index.  These will be useful for
     * 									covered queries.
     */
	public IndexDescriptorImpl(String indexType,
								boolean isUnique,
								boolean isUniqueWithDuplicateNulls,
								int[] baseColumnPositions,
								boolean[] isAscending,
								int numberOfOrderedColumns,
							   boolean excludeNulls,
							   boolean excludeDefaults
							   )
	{
		this.indexType = indexType;
		this.isUnique = isUnique;
		this.isUniqueWithDuplicateNulls = isUniqueWithDuplicateNulls;
		this.baseColumnPositions = baseColumnPositions;
		this.isAscending = isAscending;
		this.numberOfOrderedColumns = numberOfOrderedColumns;
		this.excludeNulls = excludeNulls;
		this.excludeDefaults = excludeDefaults;
	}

	/** Zero-argument constructor for Formatable interface */
	public IndexDescriptorImpl()
	{
	}

	/**
     * 
     * 
     * @see IndexDescriptor#isUniqueWithDuplicateNulls
     */
	public boolean isUniqueWithDuplicateNulls()
	{
		return isUniqueWithDuplicateNulls;
	}

	/** @see IndexDescriptor#isUnique */
	public boolean isUnique()
	{
		return isUnique;
	}

	/** @see IndexDescriptor#baseColumnPositions */
	public int[] baseColumnPositions()
	{
		return baseColumnPositions;
	}

	/** @see IndexDescriptor#getKeyColumnPosition */
	public int getKeyColumnPosition(int heapColumnPosition)
	{
		/* Return 0 if column is not in the key */
		int keyPosition = 0;

		for (int index = 0; index < baseColumnPositions.length; index++)
		{
			/* Return 1-based key column position if column is in the key */
			if (baseColumnPositions[index] == heapColumnPosition)
			{
				keyPosition = index + 1;
				break;
			}
		}

		return keyPosition;
	}

	/** @see IndexDescriptor#numberOfOrderedColumns */
	public int numberOfOrderedColumns()
	{
		return numberOfOrderedColumns;
	}

	/** @see IndexDescriptor#indexType */
	public String indexType()
	{
		return indexType;
	}

	/** @see IndexDescriptor#isAscending */
	public boolean isAscending(Integer keyColumnPosition) {
		int i = keyColumnPosition.intValue() - 1;
		if (i < 0 || i >= baseColumnPositions.length)
			return false;
		return isAscending[i];
    }

	/** @see IndexDescriptor#isDescending */
	public boolean			isDescending(Integer keyColumnPosition) {
		int i = keyColumnPosition.intValue() - 1;
		if (i < 0 || i >= baseColumnPositions.length)
			return false;
		return ! isAscending[i];
    }

	/** @see IndexDescriptor#isAscending */
	public boolean[]		isAscending()
	{
		return isAscending;
	}

	/** @see IndexDescriptor#setBaseColumnPositions */
	public void		setBaseColumnPositions(int[] baseColumnPositions)
	{
		this.baseColumnPositions = baseColumnPositions;
	}

	/** @see IndexDescriptor#setIsAscending */
	public void		setIsAscending(boolean[] isAscending)
	{
		this.isAscending = isAscending;
	}

	/** @see IndexDescriptor#setNumberOfOrderedColumns */
	public void		setNumberOfOrderedColumns(int numberOfOrderedColumns)
	{
		this.numberOfOrderedColumns = numberOfOrderedColumns;
	}

    /**
     *
     * Simple Check whether an Index Descriptor is a primary key.
     *
     * @return
     */
    @Override
    public boolean isPrimaryKey() {
        return indexType != null && indexType().contains("PRIMARY");
    }

	public String toString()
	{
		StringBuilder sb = new StringBuilder(60);

		if (isUnique)
			sb.append("UNIQUE ");
		else if (isUniqueWithDuplicateNulls)
			sb.append ("UNIQUE WITH DUPLICATE NULLS");

		sb.append(indexType);

		sb.append(" (");


		for (int i = 0; i < baseColumnPositions.length; i++)
		{
			if (i > 0)
				sb.append(", ");
			sb.append(baseColumnPositions[i]);
			if (! isAscending[i])
				sb.append(" DESC");
		}

		sb.append(")");

		if (excludeNulls) {
			sb.append(" EXCL NULLS");
		}

		if (excludeDefaults) {
			sb.append(" EXCL DEFAULTS");
		}

		return sb.toString();
	}

	/* Externalizable interface */

	/**
	 * @see java.io.Externalizable#readExternal
	 *
	 * @exception IOException	Thrown on read error
	 */
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        FormatableHashtable fh = (FormatableHashtable) in.readObject();
        isUnique = fh.getBoolean("isUnique");
        int bcpLength = fh.getInt("keyLength");
        baseColumnPositions = new int[bcpLength];
        isAscending = new boolean[bcpLength];
        for (int i = 0; i < bcpLength; i++) {
            baseColumnPositions[i] = fh.getInt("bcp" + i);
            isAscending[i] = fh.getBoolean("isAsc" + i);
        }
        numberOfOrderedColumns = fh.getInt("orderedColumns");
        indexType = (String) fh.get("indexType");
        //isUniqueWithDuplicateNulls attribute won't be present if the index
        //was created in older versions
        isUniqueWithDuplicateNulls = fh.containsKey("isUniqueWithDuplicateNulls") && fh.getBoolean("isUniqueWithDuplicateNulls");
		excludeNulls = fh.containsKey("excludeNulls") && fh.getBoolean("excludeNulls");
		excludeDefaults = fh.containsKey("excludeDefaults") && fh.getBoolean("excludeDefaults");
    }

	/**
	 * @see java.io.Externalizable#writeExternal
	 *
	 * @exception IOException	Thrown on write error
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		FormatableHashtable fh = new FormatableHashtable();
		fh.putBoolean("isUnique", isUnique);
		fh.putInt("keyLength", baseColumnPositions.length);
		for (int i = 0; i < baseColumnPositions.length; i++)
		{
			fh.putInt("bcp" + i, baseColumnPositions[i]);
			fh.putBoolean("isAsc" + i, isAscending[i]);
		}
		fh.putInt("orderedColumns", numberOfOrderedColumns);
		fh.put("indexType", indexType);
		//write the new attribut older versions will simply ignore it
		fh.putBoolean("isUniqueWithDuplicateNulls", 
                                        isUniqueWithDuplicateNulls);
		fh.putBoolean("excludeNulls",
				excludeNulls);
		fh.putBoolean("excludeDefaults",
				excludeDefaults);

        out.writeObject(fh);
	}

	/* TypedFormat interface */
	public int getTypeFormatId()
	{
		return StoredFormatIds.INDEX_DESCRIPTOR_IMPL_V02_ID;
	}

	/**
	 * Test for value equality
	 *
	 * @param other		The other indexrowgenerator to compare this one with
	 *
	 * @return	true if this indexrowgenerator has the same value as other
	 */

	public boolean equals(Object other)
	{
		/* Assume not equal until we know otherwise */
		boolean retval = false;

		/* Equal only if comparing the same class */
		if (other instanceof IndexDescriptorImpl)
		{
			IndexDescriptorImpl id = (IndexDescriptorImpl) other;

			/*
			** Check all the fields for equality except for the array
			** elements (this is hardest, so save for last)
			*/
			if ((id.isUnique == this.isUnique)       &&
                (id.isUniqueWithDuplicateNulls == 
                    this.isUniqueWithDuplicateNulls) &&
                (id.baseColumnPositions.length ==
                    this.baseColumnPositions.length) &&
                (id.numberOfOrderedColumns     == 
                    this.numberOfOrderedColumns)     &&
                (id.indexType.equals(this.indexType)))
			{
				/*
				** Everything but array elements known to be true -
				** Assume equal, and check whether array elements are equal.
				*/
				retval = true;

				for (int i = 0; i < this.baseColumnPositions.length; i++)
				{
					/* If any array element is not equal, return false */
					if ((id.baseColumnPositions[i] !=
						 this.baseColumnPositions[i]) || 
                        (id.isAscending[i] != this.isAscending[i]))
					{
						retval = false;
						break;
					}
				}
			}
		}

		return retval;
	}

	/**
	  @see java.lang.Object#hashCode
	  */
	public int hashCode()
	{
		int	retval;

		retval = isUnique ? 1 : 2;
		retval *= numberOfOrderedColumns;
        for (int baseColumnPosition : baseColumnPositions) {
            retval *= baseColumnPosition;
        }
		retval *= indexType.hashCode();

		return retval;
	}

	@Override
	public boolean excludeNulls() {
		return excludeNulls;
	}

	@Override
	public boolean excludeDefaults() {
		return excludeDefaults;
	}
}
