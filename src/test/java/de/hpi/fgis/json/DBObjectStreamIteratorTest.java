package de.hpi.fgis.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Test;

import com.mongodb.util.JSON;


public class DBObjectStreamIteratorTest {
	private DBObjectStreamIterator reader;
	private DBObjectStreamIterator readerNL;
	@Before
	public void setUp() throws Exception {
		reader = new DBObjectStreamIterator(new BufferedReader(new StringReader("{'mini':true}\nundefined\n{'hihi':1,'hoho':'2'}\n\n{'id':'last'}\n\r{")), false);
		readerNL = new DBObjectStreamIterator(new BufferedReader(new StringReader("{'mini':\ntrue}\nundefined\n{'hihi':1,\n'hoho':'2'}\n\n{'id':'last'}\n\r{")), true);
	}

	@Test  
	public void testNext() {
		assertEquals(JSON.parse("{'mini':true}"), reader.next());
		assertEquals(JSON.parse("{'hihi':1,'hoho':'2'}"), reader.next());
		assertEquals(JSON.parse("{'id':'last'}"), reader.next());
		try {
			// try to read "{"
			reader.next();
			// woops this isn' a correct JSON object
			fail();
		} catch (NoSuchElementException e) {
			// success!
		}
	}

	@Test  
	public void testNextNL() {
		assertEquals(JSON.parse("{'mini':true}"), readerNL.next());
		assertEquals(JSON.parse("{'hihi':1,'hoho':'2'}"), readerNL.next());
		assertEquals(JSON.parse("{'id':'last'}"), readerNL.next());
		try {
			// try to read "{"
			readerNL.next();
			// woops this isn' a correct JSON object
			fail();
		} catch (NoSuchElementException e) {
			// success!
		}
	}

	@Test
	public void testHasNext() {
		assertTrue(reader.hasNext());
		reader.next();
		assertTrue(reader.hasNext());
		reader.next();
		assertTrue(reader.hasNext());
		reader.next();
		assertFalse(reader.hasNext());
	}
	
	@Test
	public void testHasNextNL() {
		assertTrue(readerNL.hasNext());
		readerNL.next();
		assertTrue(readerNL.hasNext());
		readerNL.next();
		assertTrue(readerNL.hasNext());
		readerNL.next();
		assertFalse(readerNL.hasNext());
	}

	@Test(expected = IllegalStateException.class)  
	public void testRemove() {
		reader.remove();
	}

	@Test(expected = IllegalStateException.class)  
	public void testRemoveNL() {
		readerNL.remove();
	}

}
