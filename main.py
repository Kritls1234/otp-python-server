function callYopmail(email, mode) {
  const result = callPython("/get-yopmail", { email: email, mode: mode });
  
  if (!result || !result.success) {
    return fail((result && result.message) || "ไม่พบอีเมลใน Yopmail");
  }
  
  if (!result.emails || result.emails.length === 0) {
    return fail("ไม่พบอีเมลใน Yopmail");
  }
  
  const targetEmail = pickEmailForMode(result.emails, mode);
  if (!targetEmail) {
    return fail(mode === "sixdigit" ? "ไม่พบอีเมล Code 6 หลักล่าสุด" : "ไม่พบอีเมล Household ล่าสุด");
  }
  
  return parseEmailHtml(targetEmail.html || "", targetEmail);
}
