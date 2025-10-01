package mu.semte.ch.lib.utils;

import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import jakarta.servlet.http.HttpServletRequest;

import java.util.Optional;

import static java.util.Optional.ofNullable;

public interface RequestHelper {
  static Optional<HttpServletRequest> getCurrentHttpRequest() {
    return ofNullable(RequestContextHolder.getRequestAttributes())
        .filter(ServletRequestAttributes.class::isInstance)
        .map(ServletRequestAttributes.class::cast)
        .map(ServletRequestAttributes::getRequest);
  }
}
