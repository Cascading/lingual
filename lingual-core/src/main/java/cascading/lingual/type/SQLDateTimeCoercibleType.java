/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.lingual.type;

import java.lang.reflect.Type;
import java.util.Date;

import cascading.CascadingException;
import cascading.tuple.type.CoercibleType;
import cascading.util.Util;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.eigenbase.sql.type.BasicSqlType;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util14.DateTimeUtil;
import org.eigenbase.util14.ZonelessDatetime;

/**
 *
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonAutoDetect(
  fieldVisibility = JsonAutoDetect.Visibility.NONE,
  getterVisibility = JsonAutoDetect.Visibility.NONE,
  setterVisibility = JsonAutoDetect.Visibility.NONE
)
public abstract class SQLDateTimeCoercibleType extends BasicSqlType implements CoercibleType
  {
  public static final int MILLIS_PER_DAY = 86400000;

  protected SQLDateTimeCoercibleType( SqlTypeName sqlTypeName )
    {
    super( sqlTypeName );
    }

  @Override
  public Object canonical( Object value )
    {
    if( value == null )
      return null;

    Class from = value.getClass();

    if( from == String.class )
      return parse( (String) value ).getDateValue();

    if( from == Date.class )
      return ( (Date) value ).getTime(); // in UTC

    if( from == Long.class || from == long.class )
      return value;

    throw new CascadingException( "unknown type coercion requested from: " + Util.getTypeName( from ) );
    }

  @Override
  public Object coerce( Object value, Type to )
    {
    if( value == null )
      return null;

    Class from = value.getClass();

    if( from != Long.class )
      throw new IllegalStateException( "was not normalized" );

    // no coercion, or already in canonical form
    if( to == Long.class || to == long.class || to == Object.class )
      return value;

    if( to == Integer.class )
      return ( (Long) value ).intValue();

    ZonelessDatetime date = createInstance();

    date.setZonelessTime( (Long) value );

    if( to == String.class )
      return date.toString();

    if( to == java.sql.Date.class )
      return new java.sql.Date( date.getJdbcDate( DateTimeUtil.defaultZone ) );

    if( to == java.sql.Timestamp.class )
      return new java.sql.Timestamp( date.getJdbcTimestamp( DateTimeUtil.defaultZone ) );

    if( to == java.sql.Time.class )
      return new java.sql.Time( date.getJdbcTime( DateTimeUtil.defaultZone ) );

    throw new CascadingException( "unknown type coercion requested, from: " + Util.getTypeName( from ) + " to: " + Util.getTypeName( to ) );
    }

  protected abstract ZonelessDatetime parse( String value );

  protected abstract ZonelessDatetime createInstance();

  @Override
  public boolean isNullable()
    {
    return true;
    }

  @Override
  public int hashCode()
    {
    return typeName.hashCode();
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;

    if( !( object instanceof SQLDateTimeCoercibleType ) )
      return false;

    SQLDateTimeCoercibleType that = (SQLDateTimeCoercibleType) object;

    if( typeName != that.typeName )
      return false;

    return true;
    }
  }
