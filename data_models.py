from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from datetime import datetime

Base = declarative_base()


from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    ForeignKey,
    DateTime,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Patient(Base):
    __tablename__ = "patients"

    id = Column(Integer, primary_key=True)
    mrn = Column(String, nullable=False, unique=True)
    sex = Column(String, nullable=False)
    birth_date = Column(String, nullable=False)

    studies = relationship("Study", back_populates="patient")
    bmd_values = relationship("BMDValue", back_populates="patient")


class Study(Base):
    __tablename__ = "studies"

    id = Column(Integer, primary_key=True)
    patient_id = Column(Integer, ForeignKey("patients.id"), nullable=False)
    study_instance_uid = Column(String, nullable=False, unique=True)
    accession = Column(String, nullable=False, unique=True)
    date_time = Column(DateTime, nullable=False)
    description = Column(String, nullable=False)
    age = Column(Integer, nullable=False)
    size = Column(Float, nullable=True)
    weight = Column(Float, nullable=True)
    ethnicity = Column(String, nullable=True)

    patient = relationship("Patient", back_populates="studies")
    report = relationship("Report", uselist=False, back_populates="study")
    bmd_values = relationship("BMDValue", back_populates="study")


class Report(Base):
    __tablename__ = "reports"

    id = Column(Integer, primary_key=True)
    study_id = Column(Integer, ForeignKey("studies.id"), nullable=False)
    sop_instance_uid = Column(String, nullable=False, unique=True)

    study = relationship("Study", back_populates="report")
    bmd_values = relationship("BMDValue", back_populates="report")


class BMDValue(Base):
    __tablename__ = "bmd_values"

    id = Column(Integer, primary_key=True)
    report_id = Column(Integer, ForeignKey("reports.id"), nullable=False)
    study_id = Column(Integer, ForeignKey("studies.id"), nullable=False)
    patient_id = Column(Integer, ForeignKey("patients.id"), nullable=False)

    body_part = Column(String, nullable=False)  # e.g., 'spine', 'hip'
    region = Column(String, nullable=False)  # e.g., 'L1', 'L2', 'L1-L4'
    bmd = Column(Float, nullable=False)  # BMD in g/cm2
    t_score = Column(Float, nullable=True)
    z_score = Column(Float, nullable=True)

    report = relationship("Report", back_populates="bmd_values")
    study = relationship("Study", back_populates="bmd_values")
    patient = relationship("Patient", back_populates="bmd_values")
