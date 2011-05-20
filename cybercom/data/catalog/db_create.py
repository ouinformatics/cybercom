from sqlalchemy import create_engine, ForeignKeyConstraint, ForeignKey, MetaData, Column, Integer, String, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
#from sqlalchemy.schema import *

engine = create_engine('postgresql://mstacy@localhost:5432/cybercom')#, echo=True)
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()
class schema_Version(Base):
    __tablename__='schema_version'
    key = Column(String(255), primary_key = True)
    timestamp = Column(DateTime)
    extra = Column(String(255))
    sfile = Column(String(255))

class rt_State(Base):
    __tablename__='rt_state'
    state_code = Column(String(10), primary_key=True)
    state_name = Column(String(255))
    status_flag = Column(String(1), server_default='A') # 'A' Active 'I' Inactive
    remark = Column(String(2000))
class rt_Result_type(Base):
    __tablename__='rt_result_type'

    result_type_code = Column(String(20), primary_key = True)
    result_type_desc = Column(String(255))
    status_flag = Column(String(1), server_default='A') # 'A' Active 'I' Inactive
    remark = Column(String(2000))

class rt_Method(Base):
    __tablename__='rt_method'

    method_code = Column(String(50), primary_key= True)
    method_name = Column(String(255))
    method_desc = Column(String(500))
    status_flag = Column(String(1), server_default='A') # 'A' Active 'I' Inactive
    remark = Column(String(2000))
    base_method = Column(String(50))
    params = relationship("rt_Method_parameters", backref="rt_method")

class rt_Method_parameters(Base):
    __tablename__= 'rt_method_parameters'

    method_code =Column(String(50),ForeignKey("rt_method.method_code", onupdate='CASCADE', ondelete='CASCADE'), primary_key= True)
    param_id = Column(Integer, primary_key = True)
    param_type = Column(String(20))
    param_name = Column(String(255))
    param_desc = Column(String(500))
    param_value = Column(String(255))

class rt_Location_type(Base):
    __tablename__= 'rt_location_type'

    location_type_code =Column(String(50), primary_key= True)
    location_type_desc = Column(String(255))
    status_flag = Column(String(1), server_default='A') # 'A' Active 'I' Inactive
    remark = Column(String(2000))

class rt_Variables(Base):
    __tablename__= 'rt_variables'

    var_id = Column(String(30), primary_key =True)
    variable_name = Column(String(255))
    sort_order = Column(Integer)
    variable_type = Column(String(30))
    status_flag = Column(String(1), server_default='A') # 'A' Active 'I' Inactive
    var_short_name= Column(String(30))
    remark = Column(String(2000))

class rt_Unit(Base):
    __tablename__= 'rt_unit'

    unit_code = Column(String(15), primary_key=True)
    unit_desc = Column(String(255))
    unit_type = Column(String(15))
    status_flag = Column(String(1), server_default='A') # 'A' Active 'I' Inactive
    remark = Column(String(2000))

class rt_Unit_conversion(Base):
    __tablename__= 'rt_unit_conversion'

    reported_unit = Column(String(15),ForeignKey("rt_unit.unit_code"), primary_key =True)
    target_unit = Column(String(15),ForeignKey("rt_unit.unit_code"), primary_key =True)
    conversion_factor = Column(Float, nullable = False)
    delta = Column(Float, nullable = False)
    remark = Column(String(2000))

class dt_Location(Base):
    __tablename__ = 'dt_location'

    loc_id = Column(String(60), primary_key= True)
    commons_id = Column(Integer,autoincrement=False, primary_key = True)
    loc_name = Column(String(255))
    data_provider = Column(String(20))
    loc_desc = Column(String(1000))
    loc_type = Column(String(50),ForeignKey("rt_location_type.location_type_code",onupdate = "CASCADE"))
    loc_purpose = Column(String(255))
    loc_county = Column(String(80))
    loc_state = Column(String(10))
    lat = Column(Float)
    lon = Column(Float)
    northbounding = Column(Float)
    southbounding = Column(Float)
    eastbounding = Column(Float)
    westbounding = Column(Float)
    remark = Column(String(2000))
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    coord_system = Column(String(255))
    projection = Column(String(255))
    loc_order = Column(Integer)
    base_loc_id = Column(String(60))
    loc_params = relationship("dt_Location_parameter", backref="dt_location")

class dt_Location_parameter(Base):
    __tablename__ = 'dt_location_parameter'
    __table_args__ = (ForeignKeyConstraint(['commons_id','loc_id'],['dt_location.commons_id','dt_location.loc_id'],onupdate="CASCADE", ondelete="CASCADE"),{})
    commons_id = Column(Integer, primary_key = True)
    loc_id = Column(String(60), primary_key = True)
    param_id = Column(Integer, primary_key=True)
    param_name = Column(String(255))
    param_value = Column(String(255))
    param_unit = Column(String(15))
    remark = Column(String(2000))
    ForeignKeyConstraint(['commons_id','loc_id'],['dt.location.commons_id','dt_location.loc_id'],onupdate="CASCADE", ondelete="CASCADE")

class dt_Type(Base):
    __tablename__ = 'dt_type'
    type_id = Column(String(40),primary_key = True)
    type_name = Column(String(255))
    product =  Column(String(255))
    description = Column(String(255))
    resolution = Column(String(255))
    res_unit = Column(String(50))
    object_type = Column(String(100))
    object_data_type1 =  Column(String(255))
    object_data_opt1_unit = Column(String(100))

class rt_Organization_type(Base):
    __tablename__ = 'rt_organization_type'
    org_type =  Column(String(20),primary_key=True)
    org_type_desc = Column(String(255))
    status_flag = Column(String(1), server_default='A')
    remark = Column(String(2000))

class rt_Organization(Base):
    __tablename__ = 'rt_organization'
    org_code = Column(String(20), primary_key=True)
    org_name = Column(String(255))
    org_type =  Column(String(20),ForeignKey('rt_organization_type.org_type',onupdate="CASCADE"))
    status_flag = Column(String(1), server_default='A')
    remark = Column(String(2000))

class dt_Data_commons(Base):
    __tablename__ = 'dt_data_commons'
    commons_id = Column(Integer, primary_key = True)
    commons_code = Column(String(20))
    commons_type = Column(String(20))
    data_provider = Column(String(20))
    program_code = Column(String(20))
    commons_name = Column(String(60))
    commons_desc = Column(String(2000))
    start_date = Column(DateTime)
    status_flag = Column(String(1), server_default='A')

class dt_People(Base):
    __tablename__ = 'dt_people'
    people_id = Column(Integer, primary_key =True)
    person_name = Column(String(255))
    title = Column(String(100))
    address1 = Column(String(80))
    address2 = Column(String(80))
    city = Column(String(40))
    state = Column(String(10), ForeignKey("rt_state.state_code" ,onupdate="CASCADE"))
    postal_code = Column(String(15))
    country = Column(String(80))
    phone_number  = Column(String(20))
    email = Column(String(100))
    org_code = Column(String(20), ForeignKey("rt_organization.org_code" ,onupdate="CASCADE"))

class dt_Contributors(Base):
    __tablename__ = 'dt_contributors'
    commons_id = Column(Integer,ForeignKey("dt_data_commons.commons_id" ,onupdate="CASCADE"), primary_key = True)
    people_id = Column(Integer,ForeignKey("dt_people.people_id" ,onupdate="CASCADE"), autoincrement=False, primary_key = True)
    project_title = Column(String(255))
    description = Column(String(255))
    remark =  Column(String(2000))

class dt_Catalog(Base):
    __tablename__ = 'dt_catalog'
    __table_args__ = (ForeignKeyConstraint(['commons_id','loc_id'],['dt_location.commons_id','dt_location.loc_id'],onupdate="CASCADE", ondelete="CASCADE"),{})
    commons_id = Column(Integer,ForeignKey("dt_data_commons.commons_id" ,onupdate="CASCADE"), primary_key = True)
    cat_id = Column(Integer, primary_key = True)
    cat_name = Column(String(255))
    data_provider = Column(String(255))
    cat_type = Column(String(40), ForeignKey("dt_type.type_id" ,onupdate="CASCADE"))
    loc_id = Column(String(60))
    source_id = Column(Integer)
    cat_desc = Column(String(255))
    cat_method = Column(String(50), ForeignKey("rt_method.method_code" ,onupdate="CASCADE"))
    observed_date = Column(DateTime)
    remark = Column(String(2000))
    observed_year = Column(Integer)
    custom_field_1 = Column(String(255))
    custom_field_2 = Column(String(255))
    status_flag = Column(String(1), server_default='A')
    status_data = Column(String(20), server_default='N') # Y Loaded in local Database, N located on remote source
    userid = Column(String(20))
    timestamp_created = Column(DateTime)

class dt_Event(Base):
    __tablename__ = 'dt_event'
    __table_args__ = (ForeignKeyConstraint(['commons_id','cat_id'],['dt_catalog.commons_id','dt_catalog.cat_id'],onupdate="CASCADE"),ForeignKeyConstraint(['commons_id','loc_id'],['dt_location.commons_id','dt_location.loc_id'],onupdate="CASCADE", ondelete="CASCADE"),{})
    commons_id = Column(Integer,autoincrement=False, primary_key= True)
    event_id = Column(Integer, primary_key = True)
    cat_id = Column(Integer)
    event_name = Column(String(255))
    event_desc = Column(String(300))
    event_method = Column(String(50), ForeignKey("rt_method.method_code" ,onupdate="CASCADE"))
    event_date = Column(DateTime)
    event_type = Column(String(40), ForeignKey("dt_type.type_id" ,onupdate="CASCADE"))
    loc_id = Column(String(60))
    custom_1 = Column(String(300))
    remark = Column(String(2000))
    status_flag = Column(String(1), server_default='A') # A - Active I - Inactive
    userid = Column(String(20))
    timestamp_created = Column(DateTime)
class dt_Result(Base):
    __tablename__ = 'dt_result'
    __table_args__ = (ForeignKeyConstraint(['commons_id','event_id'],['dt_event.commons_id','dt_event.event_id'],onupdate="CASCADE"),{})
    commons_id = Column(Integer, autoincrement=False, primary_key= True)
    event_id = Column(Integer, autoincrement=False, primary_key = True)
    var_id = Column(String(30), ForeignKey("rt_variables.var_id" ,onupdate="CASCADE", ondelete="RESTRICT"), autoincrement=False, primary_key=True)
    result_text = Column(String(2000))
    result_numeric = Column(Float)
    result_error = Column(String(300))
    result_date = Column(DateTime)
    result_type = Column(String(20),ForeignKey("rt_result_type.result_type_code" ,onupdate="CASCADE"))
    result_unit = Column(String(15),ForeignKey("rt_unit.unit_code" ,onupdate="CASCADE"))
    result_order = Column(Integer)
    stat_type = Column(String(20))
    stat_result = Column(String(100))
    validated = Column(String(300))
    remark = Column(String(2000))
    status_flag = Column(String(1), server_default='A') # A - Active I - Inactive

Base.metadata.create_all(engine)

session.commit()
#session.add(dt_catalog('1', '200', 'Foobar', 'someguy', 'sometype', '73070', '123', 'Somedesc', 'method', datetime(2010,10,10), 'Some very long string', '2009', 'foo', 'foo', 'B', 'Y', 'mstacy', datetime( 2009, 1, 1)) )
