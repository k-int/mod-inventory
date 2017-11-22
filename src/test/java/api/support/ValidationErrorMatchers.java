package api.support;

import io.vertx.core.json.JsonObject;
import org.folio.inventory.support.JsonArrayHelper;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.List;

public class ValidationErrorMatchers {
  public static Matcher<JsonObject> errorFor(String property, String message) {
    return new TypeSafeMatcher<JsonObject>() {
      @Override
      public void describeTo(Description description) {
        description.appendText(String.format(
          "a validation error message: %s for property: %s", message, property));
      }

      @Override
      protected boolean matchesSafely(JsonObject response) {
        List<JsonObject> parameters = JsonArrayHelper.toList(
          response.getJsonArray("parameters"));

        return response.getString("message").equals(message) &&
          parameters.stream().anyMatch(p -> p.getString("key").equals(property));
      }
    };
  }
}
