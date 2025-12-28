
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{
    cell::RefCell,io::{Read, Write},rc::Rc
};

use apache_avro::{
    Reader, Schema, Writer,
    types::{Value},
};
use apache_avro_test_helper::{
    TestResult,
    init,
};

#[derive(Clone, Default)]
struct TestBuffer(Rc<RefCell<Vec<u8>>>, usize);

impl Write for TestBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.borrow_mut().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Read for TestBuffer {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let vec = self.0.borrow_mut();
        let left_to_read = vec.len() - self.1;
        let to_read = if buf.len() > left_to_read {left_to_read} else {buf.len()};
        let slice = &vec[self.1..(self.1+to_read)];
        slice.iter().enumerate().for_each(|(index, value)|{
            buf[index] = *value;
        });
        self.1 = self.1 + to_read;
        Ok(to_read)
    }
}

#[test]
fn test_long_to_int() -> TestResult {
    init();
    let writer_schema = r#"{
        "type": "record",
        "name": "writerSchema",
        "fields": [
        {
            "name": "aNumber",
            "type": "long"
        }
        ]
    }"#;

    let reader_schema = r#"{
        "type": "record",
        "name": "writerSchema",
        "fields": [
        {
            "name": "aNumber",
            "type": "int"
        }
        ]
    }"#;

    // value that was written with writer_schema
    let value = Value::Record(vec![(
            "aNumber".into(),
            Value::Long(2147483647000)
    )
    ]);

    let buffer = TestBuffer(Rc::new(RefCell::new(Vec::new())), 0);

    let writer_schema = Schema::parse_str(writer_schema)?;

    let mut writer = Writer::new(&writer_schema, buffer.clone())?;

    let _ = writer.append(value);
    let _ = writer.flush();

    let reader_schema = Schema::parse_str(reader_schema)?;
    let mut reader = Reader::with_schema(&reader_schema,buffer)?;

    let read_value = reader.next();

    match read_value {
        Some(v) => {
            panic!("Reader and writer schema are incompatible, should not be able to deserialize. Desearlized value: {:?}", v)
        }
        None => {}
    }
    Ok(())
}


#[test]
fn test_record_resolution() -> TestResult {
    // this test should fail per the specifications requirements that
    // record - record resolution checks strict equality of record names
    init();
    let writer_schema = r#"{
        "type": "record",
        "name": "schemaA",
        "fields": [
        {
            "name": "aNumber",
            "type": "int"
        }
        ]
    }"#;

    let reader_schema = r#"{
        "type": "record",
        "name": "schemaB",
        "fields": [
        {
            "name": "aNumber",
            "type": "int"
        }
        ]
    }"#;

    // value that was written with writer_schema
    let value = Value::Record(vec![(
            "aNumber".into(),
            Value::Int(2000)
    )
    ]);

    let buffer = TestBuffer(Rc::new(RefCell::new(Vec::new())), 0);

    let writer_schema = Schema::parse_str(writer_schema)?;

    let mut writer = Writer::new(&writer_schema, buffer.clone())?;

    let _ = writer.append(value);
    let _ = writer.flush();

    let reader_schema = Schema::parse_str(reader_schema)?;
    let mut reader = Reader::with_schema(&reader_schema,buffer)?;

    let read_value = reader.next();

    match read_value {
        Some(v) => {
            panic!("Reader and writer schema are incompatible, should not be able to deserialize. Desearlized value: {:?}", v)
        }
        None => {}
    }
    Ok(())
}
