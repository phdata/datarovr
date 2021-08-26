package io.phdata.snowpark.helpers

import org.junit.Assert.assertEquals
import org.scalatest.junit.JUnitSuite
import org.junit.Test

/**
 * Taken and ported from here:
 * https://stackoverflow.com/a/17369948/2639647
 */
class TestGlob extends JUnitSuite{


  @Test
  def star_becomes_dot_star(): Unit = {
    assertEquals("gl.*b", Glob("gl*b").toString)
  }

  @Test
  def escaped_star_is_unchanged(): Unit = {
    assertEquals("gl\\*b", Glob("gl\\*b").toString)
  }

  @Test
  def question_mark_becomes_dot(): Unit = {
    assertEquals("gl.b", Glob("gl?b").toString)
  }

  @Test
  def escaped_question_mark_is_unchanged(): Unit = {
    assertEquals("gl\\?b", Glob("gl\\?b").toString)
  }

  @Test
  def character_classes_dont_need_conversion(): Unit = {
    assertEquals("gl[-o]b", Glob("gl[-o]b").toString)
  }

  @Test
  def escaped_classes_are_unchanged(): Unit = {
    assertEquals("gl\\[-o\\]b", Glob("gl\\[-o\\]b").toString)
  }

  @Test
  def negation_in_character_classes(): Unit = {
    assertEquals("gl[^a-n!p-z]b", Glob("gl[!a-n!p-z]b").toString)
  }

  @Test
  def nested_negation_in_character_classes(): Unit = {
    assertEquals("gl[[^a-n]!p-z]b", Glob("gl[[!a-n]!p-z]b").toString)
  }

  @Test
  def escape_carat_if_it_is_the_first_char_in_a_character_class(): Unit = {
    assertEquals("gl[\\^o]b", Glob("gl[^o]b").toString)
  }

  @Test
  def metachars_are_escaped(): Unit = {
    assertEquals("gl..*\\.\\(\\)\\+\\|\\^\\$\\@\\%b", Glob("gl?*.()+|^$@%b").toString)
  }

  @Test
  def metachars_in_character_classes_dont_need_escaping(): Unit = {
    assertEquals("gl[?*.()+|^$@%]b", Glob("gl[?*.()+|^$@%]b").toString)
  }

  @Test
  def escaped_backslash_is_unchanged(): Unit = {
    assertEquals("gl\\\\b", Glob("gl\\\\b").toString)
  }

  @Test
  def slashQ_and_slashE_are_escaped(): Unit = {
    assertEquals("\\\\Qglob\\\\E", Glob("\\Qglob\\E").toString)
  }

  @Test
  def braces_are_turned_into_groups(): Unit = {
    assertEquals("(glob|regex)", Glob("{glob,regex}").toString)
  }

  @Test
  def escaped_braces_are_unchanged(): Unit = {
    assertEquals("\\{glob\\}", Glob("\\{glob\\}").toString)
  }

  @Test
  def commas_dont_need_escaping(): Unit = {
    assertEquals("(glob,regex),", Glob("{glob\\,regex},").toString)
  }
}
