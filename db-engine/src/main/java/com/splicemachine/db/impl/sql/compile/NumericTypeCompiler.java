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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;

import com.splicemachine.db.iapi.services.context.ContextService;

import com.splicemachine.db.iapi.services.loader.ClassFactory;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.services.info.JVMInfo;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.NumberDataValue;
import com.splicemachine.db.iapi.types.TypeId;

import com.splicemachine.db.iapi.sql.compile.TypeCompiler;

import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.Limits;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;

/**
 * This class implements TypeId for the SQL numeric datatype.
 *
 */

public final class NumericTypeCompiler extends BaseTypeCompiler
{
	/** @see TypeCompiler#interfaceName */
	public String interfaceName()
	{
		return ClassName.NumberDataValue;
	}

	/**
	 * @see TypeCompiler#getCorrespondingPrimitiveTypeName
	 */

	public String getCorrespondingPrimitiveTypeName()
	{
		/* Only numerics and booleans get mapped to Java primitives */
		int formatId = getStoredFormatIdFromTypeId();
		switch (formatId)
		{
			case StoredFormatIds.DOUBLE_TYPE_ID:
				return "double";

			case StoredFormatIds.INT_TYPE_ID:
				return "int";

			case StoredFormatIds.LONGINT_TYPE_ID:
				return "long";

			case StoredFormatIds.REAL_TYPE_ID:
				return "float";

			case StoredFormatIds.SMALLINT_TYPE_ID:
				return "short";

			case StoredFormatIds.TINYINT_TYPE_ID:
				return "byte";

			case StoredFormatIds.DECIMAL_TYPE_ID:
			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
						"unexpected formatId in getCorrespondingPrimitiveTypeName() - " + formatId);
				}
				return null;
		}
	}

	/**
	 * Get the method name for getting out the corresponding primitive
	 * Java type.
	 *
	 * @return String		The method call name for getting the
	 *						corresponding primitive Java type.
	 */
	public String getPrimitiveMethodName()
	{
		int formatId = getStoredFormatIdFromTypeId();
		switch (formatId)
		{
			case StoredFormatIds.DOUBLE_TYPE_ID:
				return "getDouble";

			case StoredFormatIds.INT_TYPE_ID:
				return "getInt";

			case StoredFormatIds.LONGINT_TYPE_ID:
				return "getLong";

			case StoredFormatIds.REAL_TYPE_ID:
				return "getFloat";

			case StoredFormatIds.SMALLINT_TYPE_ID:
				return "getShort";

			case StoredFormatIds.TINYINT_TYPE_ID:
				return "getByte";

			case StoredFormatIds.DECIMAL_TYPE_ID:
			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
						"unexpected formatId in getPrimitiveMethodName() - " + formatId);
				}
				return null;
		}
	}

	/**
	 * @see TypeCompiler#getCastToCharWidth
	 */
	public int getCastToCharWidth(DataTypeDescriptor dts)
	{
		int formatId = getStoredFormatIdFromTypeId();
		switch (formatId)
		{
			case StoredFormatIds.DECIMAL_TYPE_ID:
				// Need to have space for '-' and decimal point.
				return dts.getPrecision() + 2;

			case StoredFormatIds.DOUBLE_TYPE_ID:
				return TypeCompiler.DOUBLE_MAXWIDTH_AS_CHAR;

			case StoredFormatIds.INT_TYPE_ID:
				return TypeCompiler.INT_MAXWIDTH_AS_CHAR;

			case StoredFormatIds.LONGINT_TYPE_ID:
				return TypeCompiler.LONGINT_MAXWIDTH_AS_CHAR;

			case StoredFormatIds.REAL_TYPE_ID:
				return TypeCompiler.REAL_MAXWIDTH_AS_CHAR;

			case StoredFormatIds.SMALLINT_TYPE_ID:
				return TypeCompiler.SMALLINT_MAXWIDTH_AS_CHAR;

			case StoredFormatIds.TINYINT_TYPE_ID:
				return TypeCompiler.TINYINT_MAXWIDTH_AS_CHAR;

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
						"unexpected formatId in getCastToCharWidth() - " + formatId);
				}
				return 0;
		}
	}

	/**
	 * @see TypeCompiler#resolveArithmeticOperation
	 *
	 * @exception StandardException		Thrown on error
	 */
	public DataTypeDescriptor
	resolveArithmeticOperation(DataTypeDescriptor leftType,
								DataTypeDescriptor rightType,
								String operator)
							throws StandardException
	{
		NumericTypeCompiler higherTC;
		DataTypeDescriptor	higherType;
		boolean				nullable;
		int					precision, scale, maximumWidth;

		/*
		** Check the right type to be sure it's a number.  By convention,
		** we call this method off the TypeId of the left operand, so if
		** we get here, we know the left operand is a number.
		*/
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(leftType.getTypeId().isNumericTypeId(),
				"The left type is supposed to be a number because we're resolving an arithmetic operator");

		TypeId leftTypeId = leftType.getTypeId();
		TypeId rightTypeId = rightType.getTypeId();

		
		boolean supported = true;

		if ( ! (rightTypeId.isNumericTypeId()) )
		{
			supported = false;
		}

		if (TypeCompiler.MOD_OP.equals(operator)) {
			switch (leftTypeId.getJDBCTypeId()) {
			case java.sql.Types.TINYINT:
			case java.sql.Types.SMALLINT:
			case java.sql.Types.INTEGER:
			case java.sql.Types.BIGINT:
				break;
			default:
				supported = false;
				break;
			}
			switch (rightTypeId.getJDBCTypeId()) {
			case java.sql.Types.TINYINT:
			case java.sql.Types.SMALLINT:
			case java.sql.Types.INTEGER:
			case java.sql.Types.BIGINT:
				break;
			default:
				supported = false;
				break;
			}

		}

		if (!supported) {
			throw StandardException.newException(SQLState.LANG_BINARY_OPERATOR_NOT_SUPPORTED, 
					operator,
					leftType.getTypeId().getSQLTypeName(),
					rightType.getTypeId().getSQLTypeName()
					);
		}

		/*
		** Take left as the higher precedence if equal
		*/
		if (rightTypeId.typePrecedence() > leftTypeId.typePrecedence())
		{
			higherType = rightType;
			higherTC = (NumericTypeCompiler) getTypeCompiler(rightTypeId);
		}
		else
		{
			higherType = leftType;
			higherTC = (NumericTypeCompiler) getTypeCompiler(leftTypeId);
		}

		/* The result is nullable if either side is nullable */
		nullable = leftType.isNullable() || rightType.isNullable();
		
		int highTypeId = higherType.getTypeId().getJDBCTypeId();
		if (java.sql.Types.TINYINT == highTypeId ||
			java.sql.Types.SMALLINT == highTypeId ||
		    java.sql.Types.INTEGER == highTypeId)
		{
			return new DataTypeDescriptor(
					TypeId.getBuiltInTypeId(java.sql.Types.BIGINT),
					nullable
				);
		}
		else
		{
		
		
			/* The calculation of precision and scale should be based upon
			 * the type with higher precedence, which is going to be the result
			 * type, this is also to be consistent with maximumWidth.  Beetle 3906.
			 */
			precision = higherTC.getPrecision(operator, leftType, rightType);
			scale = higherTC.getScale(operator, leftType, rightType);

			if (higherType.getTypeId().isDecimalTypeId()) 
			{
				maximumWidth = (scale > 0) ? precision + 3 : precision + 1;

				/*
				 ** Be careful not to overflow
				 */
				if (maximumWidth < precision)
				{
					maximumWidth = Integer.MAX_VALUE;
				}
			}
			else
			{
				maximumWidth = higherType.getMaximumWidth();
			}
		
			return new DataTypeDescriptor(
					higherType.getTypeId(),
					precision,
					scale,
					nullable,
					maximumWidth
				);
		}
	}


	/** @see TypeCompiler#convertible */
	public boolean convertible(TypeId otherType, boolean forDataTypeFunction)
	{
		return (numberConvertible(otherType, forDataTypeFunction));

	}

        /**
         * Tell whether this type (numeric) is compatible with the given type.
         *
         * @param otherType     The TypeId of the other type.
         */
	public boolean compatible(TypeId otherType)
	{
		// Numbers can only be compatible with other numbers.
		return (otherType.isNumericTypeId());
	}

	/** @see TypeCompiler#storable */
	public boolean storable(TypeId otherType, ClassFactory cf)
	{
		return numberStorable(getTypeId(), otherType, cf);
	}

	/**
		Return the method name to get a Derby DataValueDescriptor
		object of the correct type. This implementation returns "getDataValue".
	*/
	String dataValueMethodName()
	{
		if (getStoredFormatIdFromTypeId() == StoredFormatIds.DECIMAL_TYPE_ID)
			return "getDecimalDataValue";
		else
			return super.dataValueMethodName();
	}

	String nullMethodName()
	{
		int formatId = getStoredFormatIdFromTypeId();
		switch (formatId)
		{
			case StoredFormatIds.DECIMAL_TYPE_ID:
				return "getNullDecimal";

			case StoredFormatIds.DOUBLE_TYPE_ID:
				return "getNullDouble";

			case StoredFormatIds.INT_TYPE_ID:
				return "getNullInteger";

			case StoredFormatIds.LONGINT_TYPE_ID:
				return "getNullLong";

			case StoredFormatIds.REAL_TYPE_ID:
				return "getNullFloat";

			case StoredFormatIds.SMALLINT_TYPE_ID:
				return "getNullShort";

			case StoredFormatIds.TINYINT_TYPE_ID:
				return "getNullByte";

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
						"unexpected formatId in nullMethodName() - " + formatId);
				}
				return null;
		}
	}

	/**
	 * Get the precision of the operation involving
	 * two of the same types.  Only meaningful for
	 * decimals, which override this.
	 *
	 * @param operator a string representing the operator,
	 *		null means no operator, just a type merge
	 * @param leftType the left type
	 * @param rightType the left type
	 *
	 * @return	the resultant precision
	 */
	private int getPrecision(String operator,
							DataTypeDescriptor leftType,
							DataTypeDescriptor rightType)
	{
		// Only meaningful for decimal
		if (getStoredFormatIdFromTypeId() != StoredFormatIds.DECIMAL_TYPE_ID)
		{
			return leftType.getPrecision();
		}

		long lscale = (long)leftType.getScale();
		long rscale = (long)rightType.getScale();
		long lprec = (long)leftType.getPrecision();
		long rprec = (long)rightType.getPrecision();
		long val;

		/*
		** Null means datatype merge.  Take the maximum
	 	** left of decimal digits plus the scale.
		*/
		if (operator == null)
		{
			val = this.getScale(operator, leftType, rightType) +
					Math.max(lprec - lscale, rprec - rscale);
		}
		else if (operator.equals(TypeCompiler.TIMES_OP))
		{
			val = lprec + rprec;
		}
		else if (operator.equals(TypeCompiler.SUM_OP))
		{
			val = lprec - lscale + rprec - rscale + 
						this.getScale(operator, leftType, rightType);
		}
		else if (operator.equals(TypeCompiler.DIVIDE_OP))
		{
			val = Math.min(NumberDataValue.MAX_DECIMAL_PRECISION_SCALE,
						   this.getScale(operator, leftType, rightType) + lprec - lscale + rprec);
		}
		/*
		** AVG, -, +
		*/
		else
		{
			/*
			** Take max scale and max left of decimal
			** plus one.
			*/
			val = this.getScale(operator, leftType, rightType) +
					Math.max(lprec - lscale, rprec - rscale) + 1;
			if (val > Limits.DB2_MAX_DECIMAL_PRECISION_SCALE)
			// then, like DB2, just set it to the max possible.
				val = Limits.DB2_MAX_DECIMAL_PRECISION_SCALE;
		}

		if (val > Integer.MAX_VALUE)
		{
			val = Integer.MAX_VALUE;
		}
		val = Math.min(NumberDataValue.MAX_DECIMAL_PRECISION_SCALE, val);
		return (int)val;
	}

	/**
	 * Get the scale of the operation involving
	 * two of the same types.  Since we don't really
	 * have a good way to pass the resultant scale
	 * and precision around at execution time, we
	 * will model that BigDecimal does by default.
	 * This is good in most cases, though we would
	 * probably like to use something more sophisticated
	 * for division.
	 *
	 * @param operator a string representing the operator,
	 *		null means no operator, just a type merge
	 * @param leftType the left type
	 * @param rightType the left type
	 *
	 * @return	the resultant precision
	 */
	private int getScale(String operator,
							DataTypeDescriptor leftType,
							DataTypeDescriptor rightType)
	{
		// Only meaningful for decimal
		if (getStoredFormatIdFromTypeId() != StoredFormatIds.DECIMAL_TYPE_ID)
		{
			return leftType.getScale();
		}

		long val;

		long lscale = (long)leftType.getScale();
		long rscale = (long)rightType.getScale();
		long lprec = (long)leftType.getPrecision();
		long rprec = (long)rightType.getPrecision();

		/*
		** Retain greatest scale, take sum of left
		** of decimal
		*/
		if (TypeCompiler.TIMES_OP.equals(operator))
		{	
			val = lscale + rscale;
		}
		else if (TypeCompiler.DIVIDE_OP.equals(operator))
		{
			/*
			** Take max left scale + right precision - right scale + 1, 
			** or 4, whichever is biggest 
			*/
			LanguageConnectionContext lcc = (LanguageConnectionContext)
				(ContextService.getContext(LanguageConnectionContext.CONTEXT_ID)); 

			// Scale: 31 - left precision + left scale - right scale
				val = Math.max(NumberDataValue.MAX_DECIMAL_PRECISION_SCALE - lprec + lscale - rscale, 
						NumberDataValue.MIN_DECIMAL_DIVIDE_SCALE);

		}
		else if (TypeCompiler.AVG_OP.equals(operator))
		{
			val = Math.max(Math.max(lscale, rscale),
						NumberDataValue.MIN_DECIMAL_DIVIDE_SCALE);
		}
		/*
		** SUM, -, + all take max(lscale,rscale)
		*/
		else
		{
			val = Math.max(lscale, rscale);
		}

		if (val > Integer.MAX_VALUE)
		{
			val = Integer.MAX_VALUE;
		}
		val = Math.min(NumberDataValue.MAX_DECIMAL_PRECISION_SCALE, val);
		return (int)val;
	}


	public void generateDataValue(MethodBuilder mb, int collationType,
			LocalField field)
	{
		if (!JVMInfo.J2ME && getTypeId().isDecimalTypeId())
		{
			// cast the value to a Number (from BigDecimal) for method resolution
			// For J2ME there is no implementation of Number for DECIMAL
			// so values are handled as thier original type, which is just
			// a String for DECIMAL constants from the parser.
			mb.upCast("java.lang.Number");
		}

		super.generateDataValue(mb, collationType, field);
	}

}
